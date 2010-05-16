#!/usr/bin/env python

"""
@file ion/services/cei/provisioner.py
@author Michael Meisinger
@author Stephen Pasco
@brief service for provisioning operational units (VM instances).
"""

from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import spawn
from ion.data.store import Store

import ion.util.procutils as pu

store = Store()

receiver = Receiver(__name__)
    
@defer.inlineCallbacks
def start():
    
    id = yield spawn(receiver)
    yield store.put('test_instrument', id)
    
    svc_mod = __import__('ion.agents.instrument_agents.SBE49', globals(), locals(), ['SBE49InstrumentAgent'])

    # Spawn instance of a service
    svc_id = yield spawn(svc_mod)
    
    yield store.put('SBE49InstrumentAgent', svc_id)
    
    yield pu.send_message(receiver, '', svc_id, 'get', ('baudrate',
                                                        'outputformat'), {})
    yield pu.send_message(receiver, '', svc_id, 'set', {'baudrate': 19200,
                                                        'outputformat': 1}, {})
    yield pu.send_message(receiver, '', svc_id, 'get', ('baudrate',
                                                         'outputformat'), {})
    yield pu.send_message(receiver, '', svc_id, 'getLifecycleState', (), {})
    yield pu.send_message(receiver, '', svc_id, 'setLifecycleState', {})

def receive(content, msg):
    print 'in receive ', content, msg

receiver.handle(receive)
