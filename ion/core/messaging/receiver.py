#!/usr/bin/env python

"""
@file ion/core/messaging/receiver.py
@author Dorian Raymer
@author Michael Meisinger
"""

import os
import types

from zope.interface import implements, Interface
from twisted.internet import defer, reactor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


from ion.core import ioninit
from ion.core.id import Id
from ion.core.intercept.interceptor import Invocation
from ion.core.messaging import messaging
from ion.util.state_object import BasicLifecycleObject
import ion.util.procutils as pu
from ion.core.object.codec import ION_R1_GPB

from ion.util.context import ContextObject

from ion.core.exception import IonError


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

    non_rpc_index = 0

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
        log.info('Start Receiver.Receive on proc: %s' % str(self.process))


        if self.rec_shutoff:
            log.warn("MESSAGE RECEIVED AFTER SHUTOFF - DROPPED")
            log.warn("Dropped message: "+str(msg.payload))
            # @todo ACK for now. Should be requeue.
            yield msg.ack()
            defer.returnValue()

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
                sender = data.get('sender', None)

                if hasattr(self.process, 'workbench'):
                    workbench = self.process.workbench
                    process = self.process

                    log.info('Process "%s" Receiver Message Headers: OP - %s, Sender - %s, Convid - %s, Performative - %s, Protocol - %s' % (process.proc_name, op, sender, convid, performative, protocol))


                    if protocol != 'rpc':
                        # if it is not an rpc conversation - set the context

                        Receiver.non_rpc_index += 1
                        convid = 'Non RPC request ID %d' % self.non_rpc_index

                        log.info('Setting NON RPC request workbench_context: %s, in Proc: %s ' % (convid, self.process))

                        process.context = process.conversation_context.create_context(convid)

                    elif performative == 'request':
                        # if it is an rpc request - set the context
                        log.info('Setting RPC request workbench_context: %s, in Proc: %s ' % (convid, self.process))

                        process.context = process.conversation_context.create_context(convid)

                    else:
                        #log.warn('Message headers: \n%s' % pu.pprint_to_string(data))
                        log.info('Dont set context if it is not a request: %s, in Proc: %s ' % (convid, self.process))

                        try:
                            process.context = process.conversation_context.get_context(convid)
                        except KeyError, ke:

                            if self.name not in data['receiver']:
                                log.info('Recieved an RPC message for which I have no conversation - I am eavesdropping on another conversation!')
                            else:
                                log.exception("Invalid convid which has no context: \nMessage Content - %s\nConversation Context - %s " % (str(data.items()), str(process.conversation_context)))
                                raise ReceiverError('Could not set Conversation Context!')


                    log.info('Receiver Context: %s' % str(self.process.context))

                else:
                    workbench = None
                    process = None

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


                    if workbench is not None:

                        log.info('After Message Handler: Process "%s" Receiver Message Headers: OP - %s, Convid - %s, Performative - %s, Protocol - %s' % (process.proc_name, op, convid, performative, protocol))

                        log.info('Receiver Context: %s' % str(process.context))

                        # Try to remove the conversation from the conversation dictionary - no matter what we are done with this convid...
                        try:
                            process.conversation_context.remove(convid)
                        except KeyError, ke:

                            if self.name not in data['receiver']:
                                log.info('Conversation context was not registered... but I am just eavesdropping')
                            else:
                                log.info('Conversation context was not registered... something is screwy with this conversation')

                        # Cleanup the workbench after an op...
                        if protocol != 'rpc':
                            # if it is not an rpc conversation - clean up the context
                            log.info('Clearing Non RPC request workbench_context: %s, in Proc: %s ' % (convid, process))

                            # Clear anything created in this context
                            workbench.manage_workbench_cache(convid)

                            # Reset the context to something sensible on the way our - but what?
                            if process.context.get('progenitor_convid') == convid:
                                last_context = process.conversation_context.replace_context()

                                if last_context is None:
                                    # If there are no active conversations reset to a default
                                    process.context = ContextObject()
                                    # Lets try to clear everything!
                                    workbench.manage_workbench_cache()


                                else:
                                    process.context = last_context


                            count = workbench.count_persistent()
                            if count > 0:
                                    # Print a warning if someone else is using the persistence tricks...
                                log.warn('The "%s" process is holding persistent state in %d repository objects!' % (process.proc_name, count))



                        elif performative == 'request':
                            # if it is the end of an rpc request - clean up the context

                            log.info('Clearing RPC request workbench_context: %s, in Proc: %s ' % (convid, process))

                            # Clear anything created in this context
                            workbench.manage_workbench_cache(convid)

                            # Reset the context to something sensible on the way our - but what?
                            if process.context.get('progenitor_convid') == convid:
                                last_context = process.conversation_context.replace_context()

                                if last_context is None:
                                    # If there are no active conversations reset to a default
                                    process.context = ContextObject()
                                    # Lets try to clear everything!
                                    workbench.manage_workbench_cache()

                                else:
                                    process.context = last_context

                            count = workbench.count_persistent()
                            if count > 0:
                                    # Print a warning if someone else is using the persistence tricks...
                                log.warn('The "%s" process is holding persistent state in %d repository objects!' % (process.proc_name, count))



                        else:
                            log.info('No context to clear in Proc: %s ' % (process))


                        log.info(workbench.cache_info())


        log.info( 'End Receiver.Receive on proc: %s' % str(self.process))
        defer.returnValue(None)

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

                if hasattr(self.process, 'context') and msg.get('protocol') == 'rpc' and msg.get('performative') == 'request':
                    self.process.conversation_context.reference_context(msg.get('conv-id'), self.process.context)

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
