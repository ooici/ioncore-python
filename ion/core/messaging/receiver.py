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
    This thing interprets message (objects) and figures out which handler
    function (defined by the application) to invoke.
    It is also the mediator between app code, and Spawnable/Container.

    An instance of Receiver uses name of a callable to deliver a message
    from the mailbox.

    Context:
        This is the messaging perspective of an instance of Spawnable.
    """
    implements(IReceiver)

    def __init__(self, container):
        self.container = container

    def __init__(self, label, name=None, process=None, group=None):
        """
        @param label descriptive label of the module or function
        @param name the actual name, if specified. else, use
        spawned id
        """
        BasicLifecycleObject.__init__(self)

        self.label = label
        self.name = name
        self.process = process
        self.group = group

        self.handlers = []
        self.consumer = None

        self.initialize()

    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.name, "Receiver must have a name or be ProcessReceiver"
        procid = str(self.process.id)
        xnamestore = ioninit.container_instance.exchange_manager.exchange_space.store
        name_config = yield xnamestore.get(procid)
        if not name_config:
            raise RuntimeError("Messaging name undefined: "+self.name)

        consumer = yield ioninit.container_instance.new_consumer(name_config, self.receive)
        self.consumer = consumer
        print "activate",

    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        return self.consumer.close()

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
            d = defer.maybeDeferred(handler, data, msg)

    def __str__(self):
        res = "Receiver(label=%s,name=%s,group=%s)" % (self.label, self.name, self.group)

class ProcessReceiver(Receiver):
    """
    A ProcessReceiver is a Receiver that is exclusive to a process. It does
    not require keeping track of specific attributes.
    """

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        procid = str(self.process.id)
        if not self.name:
            self.name = procid

        name_config = messaging.process(procid)
        name_config.update({'name_type':'process'})
        xnamestore = ioninit.container_instance.exchange_manager.exchange_space.store
        yield xnamestore.put(procid, name_config)

        yield Receiver.on_activate(self, *args, **kwargs)

class NameReceiver(Receiver):
    pass

class ServiceReceiver(Receiver):
    pass
