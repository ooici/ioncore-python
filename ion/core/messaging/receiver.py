#!/usr/bin/env python

"""
@file ion/core/messaging/receiver.py
@author Dorian Raymer
@author Michael Meisinger
"""

import os
import types

from zope.interface import implements, Interface
from twisted.internet import defer, threads, reactor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.id import Id
from ion.core.intercept.interceptor import Invocation
from ion.core.messaging import messaging
from ion.util.state_object import BasicLifecycleObject
import ion.util.procutils as pu
from ion.core.object.codec import ION_R1_GPB

from ion.core.exception import IonError

# Static entry point for "thread local" context storage during request
# processing, eg. to retaining user-id from request message
from ion.core.ioninit import request


class ReceiverError(IonError):
    """
    An exception class for errors thrown in the receiver.
    """



class IReceiver(Interface):
    """
    Interface for a receiver on an Exchange name.
    """

class Receiver(BasicLifecycleObject):
    """
    Manages the inbound mailbox for messages. This includes the broker side
    queue (and its bindings) as well as the queue-consumers that actually
    take messages from the queue. Subclasses provide type specific behavior

    States:
    - NEW: Receiver configured
    - READY: Queues and bindings declared on the message broker; no consume
    - ACTIVE: Consumer declared and message handler callback enabled
    """
    implements(IReceiver)

    SCOPE_GLOBAL = 'global'
    SCOPE_SYSTEM = 'system'
    SCOPE_LOCAL = 'local'

    # Debugging information
    rec_messages = {}
    rec_shutoff = False

    def __init__(self, name, scope='global', label=None, xspace=None, process=None, group=None, handler=None, error_handler=None, raw=False, consumer_config=None, publisher_config=None):
        """
        @param label descriptive label for the receiver
        @param name the actual exchange name. Used for routing
        @param xspace the name of the exchange space. None for default
        @param scope name scope. One of 'global', 'system' or 'local'
        @param process IProcess instance that the receiver belongs to
        @param group a string grouping multiple receivers
        @param handler a callable for the message handler, shorthand for add_handler
        @param raw if True do not put through receive Interceptors
        @param consumer_config  Additional Consumer configuration params. Used by _init_receiver, these params take precedence over any
                                other config.
        @param publisher_config Additional Publisher configuration params, used by send()
        """
        BasicLifecycleObject.__init__(self)

        self.label = label
        self.name = name
        # @todo scope and xspace are overlapping. Use xspace and map internally?
        self.scope = scope
        self.xspace = xspace
        self.process = process
        self.group = group
        self.raw = raw
        self.consumer_config  = consumer_config if consumer_config is not None else {}
        self.publisher_config = publisher_config if publisher_config is not None else {}

        self.handlers = []
        self.error_handlers = []
        self.consumer = None

        if handler:
            self.add_handler(handler)

        if error_handler:
            self.add_error_handler(error_handler)

        self.xname = pu.get_scoped_name(self.name, self.scope)

        # A dict of messages in current processing. NOTE: Should be a queue
        self.processing_messages = {}
        # A Deferred to await processing completion after of deactivate
        self.completion_deferred = None

    @defer.inlineCallbacks
    def attach(self, *args, **kwargs):
        """
        @brief Boilderplate method that calls initialize and activate
        """
        yield self.initialize(*args, **kwargs)
        yield self.activate(*args, **kwargs)
        defer.returnValue(self.xname)

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @brief Declare the queue and binding only.
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"
        container = ioninit.container_instance
        xnamestore = container.exchange_manager.exchange_space.store
        name_config = yield xnamestore.get(self.xname)
        if not name_config:
            raise RuntimeError("Messaging name undefined: "+self.xname)

        yield self._init_receiver(name_config)
        log.debug("Receiver %s initialized (queue attached) cfg=%s" % (self.xname,name_config))

    @defer.inlineCallbacks
    def _init_receiver(self, receiver_config, store_config=False):
        container = ioninit.container_instance

        # copy and update receiver_config with the stored consumer_config
        receiver_config = receiver_config.copy()
        receiver_config.update(self.consumer_config)

        if store_config:
            xnamestore = container.exchange_manager.exchange_space.store
            yield xnamestore.put(self.xname, receiver_config)

        self.consumer = yield container.new_consumer(receiver_config)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @brief Activate the consumer.
        @retval Deferred
        """
        #self.consumer.register_callback(self.receive)
        yield self.consumer.consume(self.receive)
        log.debug("Receiver %s activated (consumer enabled)" % self.xname)

    #@defer.inlineCallbacks
    def on_deactivate(self, *args, **kwargs):
        """
        @brief Deactivate the consumer.
        @retval Deferred
        """
        #yield self.consumer.cancel()
        self.consumer.callback = None
        self.completion_deferred = defer.Deferred()

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        yield self.consumer.close()

    def on_error(self, cause= None, *args, **kwargs):
        if cause:
            log.error("Receiver error: %s" % cause)
            pass
        else:
            raise RuntimeError("Illegal state change")

    def _await_message_processing(self, term_msg_id=None):
        # HACK: Ignore the current op_terminate message
        if term_msg_id in self.processing_messages:
            del self.processing_messages[term_msg_id]

        if len(self.processing_messages) == 0:
            return

        return self.completion_deferred

    def add_handler(self, callback):
        self.handlers.append(callback)

    handle = add_handler


    def add_error_handler(self, callback):
        self.error_handlers.append(callback)

    @defer.inlineCallbacks
    def receive(self, msg):
        """
        @brief entry point for received messages; callback from Carrot. All
                registered handlers will be called in sequence
        @note is called from carrot as normal method; no return expected
        @param msg instance of carrot.backends.txamqp.Message
        """

        # Wrapping the handler in a thread to allow thread-local context during message processing.
        def do_receive_and_wait():
            threads.blockingCallFromThread(reactor, self._do_receive, msg)

        yield threads.deferToThread(do_receive_and_wait)

    @defer.inlineCallbacks
    def _do_receive(self, msg):
        """
        @brief entry point for received messages; callback from Carrot. All
                registered handlers will be called in sequence
        @note is called from carrot as normal method; no return expected
        @param msg instance of carrot.backends.txamqp.Message
        """
        log.info('Start Receiver.Receive on proc: %s' % str(self.process))


        if self.rec_shutoff:
            log.warn("MESSAGE RECEIVED AFTER SHUTOFF - DROPPED")
            log.warn("Dropped message: "+str(msg.payload))
            # @todo ACK for now. Should be requeue.
            yield msg.ack()
            return

        assert not id(msg) in self.rec_messages, "Message already consumed"
        self.rec_messages[id(msg)] = msg
        assert not id(msg) in self.processing_messages, "Message already consumed"
        self.processing_messages[id(msg)] = msg

        # Put message through the container interceptor system
        org_msg = msg
        data = msg.payload
        if not self.raw:
            inv = Invocation(path=Invocation.PATH_IN,
                             message=msg,
                             content=data,
                             process=self.process,
                             )
            inv1 = yield ioninit.container_instance.interceptor_system.process(inv)
            msg = inv1.message
            data = inv1.content

            # Interceptor failed message.  Call error handler(s)
            if inv1.status != Invocation.STATUS_PROCESS:
                log.info("Message error! to=%s op=%s" % (data.get('receiver',None), data.get('op',None)))
                try:
                    for error_handler in self.error_handlers:
                        yield defer.maybeDeferred(error_handler, data, msg, inv1.code)
                finally:
                    del self.rec_messages[id(msg)]
            else:


                # Extract message headers
                convid = data.get('conv-id', None)
                protocol = data.get('protocol', None)
                performative = data.get('performative', None)
                op = data.get('op', None)

                if hasattr(self.process, 'workbench'):
                    workbench = self.process.workbench
                else:
                    workbench = None


                current_context = request.get('workbench_context', [])

                # Set the stack local context for known entries
                #request.convid = convid
                #request.protocol = protocol
                #request.performative = performative


                log.debug( 'BEFORE YIELD to Message Handler')
                log.debug('OP "%s"' % op)
                log.debug('CONVID "%s"' %  convid)
                log.debug('PERFORMATIVE "%s"' % performative)
                log.debug('PROTOCOL "%s"' % protocol)
                log.debug('Current Context "%s"' % str(current_context))

                log.debug("WORKBENCH STATE before incoming message is added:\n%s" % str(workbench))

                if protocol != 'rpc':
                    # if it is not an rpc conversation - set the context

                    log.info('Setting NON RPC request workbench_context: %s, in Proc: %s ' % (convid, self.process))
                    current_context.append( convid)
                    request.workbench_context = current_context

                elif performative == 'request':
                    # if it is an rpc request - set the context
                    log.info('Setting RPC request workbench_context: %s, in Proc: %s ' % (convid, self.process))

                    current_context.append( convid)
                    request.workbench_context = current_context

                # if it is an RPC result message - do not set the context!


                # If this is a GPB message add it to the process workbench
                encoding = data.get('encoding', None)
                if encoding == ION_R1_GPB:

                    if workbench is None:
                        raise ReceiverError('Can not receive a GPB message in a process which does not have a workbench!')

                    # The Codec does not attach the repository to the process. That is done here.
                    content = data.get('content')
                    workbench.put_repository(content.Repository)

                    log.debug("WORKBENCH STATE after incoming message is added:\n%s" % str(workbench))


                # Make the calls into the application code (e.g. process receive)
                try:
                    for handler in self.handlers:
                        yield defer.maybeDeferred(handler, data, msg)
                finally:


                    if msg._state == "RECEIVED":
                        log.error("Message has not been ACK'ed at the end of processing")
                    del self.rec_messages[id(msg)]
                    if id(org_msg) in self.processing_messages:
                        del self.processing_messages[id(org_msg)]
                    if self.completion_deferred and len(self.processing_messages) == 0:
                        self.completion_deferred.callback(None)
                        self.completion_deferred = None

                    # Cleanup the workbench after an op...

                    if protocol != 'rpc':
                        # if it is not an rpc conversation - set the context
                        workbench_context = current_context.pop()
                        log.info('Popping Non RPC request workbench_context: %s, in Proc: %s ' % (workbench_context, self.process))

                    elif performative == 'request':
                        # if it is an rpc request - set the context

                        workbench_context = current_context.pop()
                        log.info('Popping RPC request workbench_context: %s, in Proc: %s ' % (workbench_context, self.process))

                        # if it is an RPC result message - do not set the context!

                    else:
                        # @TODO - SHOULD THIS BE HERE?
                        workbench_context = pu.get_last_or_default(current_context, 'No Context Set!')
                        log.info('Using last workbench_context: %s, in Proc: %s ' % (workbench_context, self.process))
                        #print 'CONVID:', convid
                        #print 'CONTEXT:', workbench_context


                    if hasattr(self.process, 'workbench'):

                        log.debug('AFTER YIELD to message handler')
                        log.debug('OP "%s"' % op)
                        log.debug('CONVID: %s' % convid)
                        log.debug('PERFORMATIVE: %s',performative)
                        log.debug('PROTOCOL "%s"' % protocol)
                        log.debug('Current CONTXT: %s' % current_context)
                        log.debug('WORKBENCH CONTXT: %s' % workbench_context)




                        if convid == workbench_context:

                            log.info('Receiver Process: Calling workbench clear:')

                            log.debug("WORKBENCH STATE Before Clear:\n%s" % str(self.process.workbench))

                            self.process.workbench.manage_workbench_cache(workbench_context)

                            nrepos = len(self.process.workbench._repos)
                            if  nrepos > 0:

                                pname = self.process.proc_name

                                count = 0
                                for repo in self.process.workbench._repos.itervalues():
                                    #if repo.convid_context != 'Test runner context!' or repo.persistent is True:
                                    if repo.persistent is True:
                                        count +=1

                                if count > 0:

                                    # Print a warning if someone else is using the persistence tricks...
                                    log.warn('The "%s" process is holding persistent state in %d repository objects!' % (pname, count))

                            log.debug("WORKBENCH STATE After Clear:\n%s" % str(self.process.workbench))

                        else:
                            log.debug('Workbench context does not match the Convid - Do not clear anything from the workbench!')

        log.info( 'End Receiver.Receive on proc: %s' % str(self.process))

    @defer.inlineCallbacks
    def send(self, **kwargs):
        """
        Constructs a standard message with standard headers and sends on given
        receiver.
        @param sender sender name of the message
        @param recipient recipient name of the message NOTE: this gets translated to "receiver" in the message in IONMessageInterceptor
        @param operation the operation (performative) of the message
        @param content the black-box content of the message
        @param headers dict with headers that may override standard headers
        """
        msg = kwargs
        msg['sender'] = msg.get('sender', self.xname)
        #log.debug("Send message op="+operation+" to="+str(recv))
        try:
            if not self.raw:
                inv = Invocation(path=Invocation.PATH_OUT,
                                 message=msg,
                                 content=msg['content'],
                                 )
                inv1 = yield ioninit.container_instance.interceptor_system.process(inv)
                msg = inv1.message

            # TODO fix this
            # For now, silently dropping message
            if inv1.status == Invocation.STATUS_DROP:
                log.info("Message dropped! to=%s op=%s" % (msg.get('receiver',None), msg.get('op',None)))
            else:
                # call flow: Container.send -> ExchangeManager.send -> ProcessExchangeSpace.send
                yield ioninit.container_instance.send(msg.get('receiver'), msg, publisher_config=self.publisher_config)
        except Exception, ex:
            log.exception("Send error")
        else:
            if inv1.status != Invocation.STATUS_DROP:
                log.info("===Message SENT! >>>> %s -> %s: %s:%s:%s===" % (msg.get('sender',None),
                                msg.get('receiver',None), msg.get('protocol',None),
                                msg.get('performative',None), msg.get('op',None)))
                defer.returnValue(msg)
                #log.debug("msg"+str(msg))

    def __str__(self):
        return "Receiver(label=%s,xname=%s,group=%s)" % (
                self.label, self.xname, self.group)

class ProcessReceiver(Receiver):
    """
    A ProcessReceiver is a Receiver that is exclusive to a process.
    """

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"

        name_config = messaging.process(self.xname)
        name_config.update({'name_type':'process'})

        yield self._init_receiver(name_config, store_config=True)

class WorkerReceiver(Receiver):
    """
    A WorkerReceiver is a Receiver from a worker queue.
    """

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"

        name_config = messaging.worker(self.xname)
        name_config.update({'name_type':'worker'})

        yield self._init_receiver(name_config, store_config=True)

class FanoutReceiver(Receiver):
    """
    A FanoutReceiver is a Receiver from a fanout exchange.
    """

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"

        name_config = messaging.fanout(self.xname)


        yield self._init_receiver(name_config, store_config=True)

class NameReceiver(Receiver):
    pass

class ServiceWorkerReceiver(WorkerReceiver):
    pass
