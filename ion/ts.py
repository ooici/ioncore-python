#!/usr/bin/env python

"""
@file ion/ts.py
@author Michael Meisinger
@package ion  test service with short packet and module name
"""

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import ion.util.procutils as pu

store = Store()

receiver = Receiver(__name__)

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('ts', id)

def receive(content, msg):
    print 'in receive ', content, msg
    pu.print_attributes(msg)

      
receiver.handle(receive)
