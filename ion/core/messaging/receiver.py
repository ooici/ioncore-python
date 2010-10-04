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
from ion.core.messaging import messaging
from ion.util.state_object import BasicLifecycleObject

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

    def __init__(self, label, name, process=None, group=None, handler=None):
        """
        @param label descriptive label
        @param name the actual name in the exchange. Used for routing
        """
        BasicLifecycleObject.__init__(self)

        self.label = label
        self.name = name
        self.process = process
        self.group = group

        self.handlers = []
        self.consumer = None

        if handler:
            self.add_handler(handler)

    @defer.inlineCallbacks
    def attach(self, *args, **kwargs):
        """
        @brief Boilderplate method that calls initialize and activate
        """
        yield self.initialize(*args, **kwargs)
        yield self.activate(*args, **kwargs)
        defer.returnValue(self.name)

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @brief Declare the queue and binding only.
        @retval Deferred
        """
        assert self.name, "Receiver must have a name"
        container = ioninit.container_instance
        xnamestore = container.exchange_manager.exchange_space.store
        name_config = yield xnamestore.get(self.name)
        if not name_config:
            raise RuntimeError("Messaging name undefined: "+self.name)

        yield self._init_receiver(name_config)
        #log.debug("Receiver %s initialized (queue attached) cfg=%s" % (self.name,name_config))

    @defer.inlineCallbacks
    def _init_receiver(self, receiver_config, store_config=False):
        container = ioninit.container_instance
        if store_config:
            xnamestore = container.exchange_manager.exchange_space.store
            yield xnamestore.put(self.name, receiver_config)

        self.consumer = yield container.new_consumer(receiver_config)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @brief Activate the consumer.
        @retval Deferred
        """
        self.consumer.register_callback(self.receive)
        yield self.consumer.iterconsume()
        #log.debug("Receiver %s activated (consumer enabled)" % self.name)

    @defer.inlineCallbacks
    def on_deactivate(self, *args, **kwargs):
        """
        @brief Deactivate the consumer.
        @retval Deferred
        """
        yield self.consumer.cancel()
        self.consumer.callbacks.remove(self.receive)

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        yield self.consumer.close()

    def on_error(self, *args, **kwargs):
        raise RuntimeError("Illegal state change")

    def add_handler(self, callback):
        self.handlers.append(callback)

    handle = add_handler

    # move these messaging related things to Receiver
    def receive(self, msg):
        """
        @brief entry point for received messages; callback from Carrot. All
                registered handlers will be called in sequence
        @param msg instance of carrot.backends.txamqp.Message
        """
        data = msg.payload
        for handler in self.handlers:
            # @todo call handlers in sequence (chain of callbacks)
            d = defer.maybeDeferred(handler, data, msg)

    def __str__(self):
        return "Receiver(label=%s,name=%s,group=%s)" % (
                self.label, self.name, self.group)

class ProcessReceiver(Receiver):
    """
    A ProcessReceiver is a Receiver that is exclusive to a process.
    """

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.name, "Receiver must have a name"

        name_config = messaging.process(self.name)
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
        assert self.name, "Receiver must have a name"

        name_config = messaging.worker(self.name)
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
        assert self.name, "Receiver must have a name"

        name_config = messaging.fanout(self.name)
        name_config.update({'name_type':'fanout'})

        yield self._init_receiver(name_config, store_config=True)

class NameReceiver(Receiver):
    pass

class ServiceReceiver(Receiver):
    pass
