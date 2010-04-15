#!/usr/bin/env python

"""
@file ion/services/coi/service_registry.py
@author Michael Meisinger
@package ion.services.coi service for registering service (types and instances).
"""

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

store = Store()

datastore = Store()

receiver = Receiver(__name__)

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('service_registry', id)


def receive(content, msg):
    print 'in receive ', content, msg

receiver.handle(receive)
