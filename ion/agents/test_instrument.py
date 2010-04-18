#!/usr/bin/env python

"""
@file ion/services/cei/provisioner.py
@author Michael Meisinger
@author Stephen Pasco
@package ion.agents service for provisioning operational units (VM instances).
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
    yield store.put('test_instrument', id)
    
    svc_mod = __import__('ion.agents.instrument_agent', globals(), locals(), ['instrument_agent'])

    # Spawn instance of a service
    svc_id = yield spawn(svc_mod)
    
    yield store.put('instrument_agent', svc_id)
    
    yield pu.send_message(receiver, '', svc_id, 'get', {'key':'obj1'}, {})

def receive(content, msg):
    print 'in receive ', content, msg

receiver.handle(receive)
