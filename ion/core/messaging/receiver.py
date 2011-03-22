#!/usr/bin/env python

"""
@file ion/core/messaging/receiver.py
@author Dorian Raymer
@author Michael Meisinger
"""

import os
import types

from zope.interface import implements, Interface
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.id import Id
from ion.core.intercept.interceptor import Invocation
from ion.core.messaging import messaging
from ion.util.state_object import BasicLifecycleObject
import ion.util.procutils as pu

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

    def __init__(self, name, scope='global', label=None, xspace=None, process=None, group=None, handler=None, raw=False, consumer_config={}, publisher_config={}):
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
        self.consumer_config  = consumer_config
        self.publisher_config = publisher_config

        self.handlers = []
        self.consumer = None

        if handler:
            self.add_handler(handler)

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
        #log.debug("Receiver %s initialized (queue attached) cfg=%s" % (self.xname,name_config))

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
        self.consumer.register_callback(self._receive)
        yield self.consumer.iterconsume()
        #log.debug("Receiver %s activated (consumer enabled)" % self.xname)

    @defer.inlineCallbacks
    def on_deactivate(self, *args, **kwargs):
        """
        @brief Deactivate the consumer.
        @retval Deferred
        """
        yield self.consumer.cancel()
        self.consumer.callbacks.remove(self._receive)
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

    @defer.inlineCallbacks
    def _await_message_processing(self):
        return self.completion_deferred

    def add_handler(self, callback):
        self.handlers.append(callback)

    handle = add_handler

    def _receive(self, msg):
        """
        @brief entry point for received messages; callback from Carrot. All
                registered handlers will be called in sequence
        @note is called from carrot as normal method; no return expected
        @param msg instance of carrot.backends.txamqp.Message
        """
        d = self.receive(msg)
        return d

    @defer.inlineCallbacks
    def receive(self, msg):
        """
        @brief entry point for received messages; callback from Carrot. All
                registered handlers will be called in sequence
        @note is called from carrot as normal method; no return expected
        @param msg instance of carrot.backends.txamqp.Message
        """
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
            wb = None
            if hasattr(self.process, 'workbench'):
                wb = self.process.workbench
            inv = Invocation(path=Invocation.PATH_IN,
                             message=msg,
                             content=data,
                             workbench=wb)
            inv1 = yield ioninit.container_instance.interceptor_system.process(inv)
            msg = inv1.message
            data = inv1.content

        # Make the calls into the application code (e.g. process receive)
        try:
            for handler in self.handlers:
                yield defer.maybeDeferred(handler, data, msg)
        finally:
            if org_msg._state == "RECEIVED":
                log.error("Message has not been ACK'ed at the end of processing")
            del self.rec_messages[id(org_msg)]
            del self.processing_messages[id(org_msg)]
            if self.completion_deferred and len(self.processing_messages) == 0:
                self.completion_deferred.callback(None)
                self.completion_deferred = None

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
                wb = None
                if hasattr(self.process, 'workbench'):
                    wb = self.process.workbench
                inv = Invocation(path=Invocation.PATH_OUT,
                                 message=msg,
                                 content=msg['content'],
                                 workbench=wb)
                inv1 = yield ioninit.container_instance.interceptor_system.process(inv)
                msg = inv1.message

            # call flow: Container.send -> ExchangeManager.send -> ProcessExchangeSpace.send
            yield ioninit.container_instance.send(msg.get('receiver'), msg, publisher_config=self.publisher_config)
        except Exception, ex:
            log.exception("Send error")
        else:
            log.info("Message sent! to=%s op=%s" % (msg.get('receiver',None), msg.get('op',None)))
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
        name_config.update({'name_type':'fanout'})

        yield self._init_receiver(name_config, store_config=True)

class NameReceiver(Receiver):
    pass

class ServiceWorkerReceiver(WorkerReceiver):
    pass
