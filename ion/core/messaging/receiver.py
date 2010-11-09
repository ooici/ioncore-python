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

    def __init__(self, name, scope='global', label=None, xspace=None, process=None, group=None, handler=None, raw=False):
        """
        @param label descriptive label for the receiver
        @param name the actual exchange name. Used for routing
        @param xspace the name of the exchange space. None for default
        @param scope name scope. One of 'global', 'system' or 'local'
        @param process IProcess instance that the receiver belongs to
        @param group a string grouping multiple receivers
        @param handler a callable for the message handler, shorthand for add_handler
        @param raw if True do not put through receive Interceptors
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

        self.handlers = []
        self.consumer = None

        if handler:
            self.add_handler(handler)

        self.xname = pu.get_scoped_name(self.name, self.scope)

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
            return

        assert not id(msg) in self.rec_messages, "Message already consumed"
        self.rec_messages[id(msg)] = msg

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
            if msg._state == "RECEIVED":
                log.error("Message has not been ACK'ed at the end of processing")
            del self.rec_messages[id(msg)]

    @defer.inlineCallbacks
    def send(self, **kwargs):
        """
        Constructs a standard message with standard headers and sends on given
        receiver.
        @param sender sender name of the message
        @param recipient recipient name of the message
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
            yield ioninit.container_instance.send(msg.get('receiver'), msg)
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
