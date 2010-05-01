#!/usr/bin/env python

"""
@file ion/services/cei/provisioner.py
@author Michael Meisinger
@author Stephen Pasco
@author Steve Foley
@package ion.agents service for provisioning operational units (VM instances).
"""

import logging
from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store
from ion.test.iontest import IonTestCase
from ion.core.base_process import RpcClient

import ion.util.procutils as pu

logging.basicConfig(level=logging.DEBUG)

class TestInstrumentAgent(IonTestCase, RpcClient):
    
    def __init__(self):
        RpcClient.__init__(self)
    
    @defer.inlineCallbacks
    def setUp(self):
        IonTestCase.setUp(self)
        self.store = yield Store()
        yield self.attach()
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield IonTestCase.tearDown(self)
    
    @defer.inlineCallbacks
    def testGetSetSBE49Params(self):
        """
        Test the ability of the SBE49 driver to send and receive get, set,
        and other messages. Best called as RPC message pairs.
        """
        yield self.store.put('test_instrument', id)
    
        svc_mod = __import__('ion.agents.instrumentagents.SBE49', globals(), \
                             locals(), ['SBE49InstrumentAgent'])

        # Spawn instance of a service
        svc_id = yield spawn(svc_mod)
    
        yield self.store.put('SBE49InstrumentAgent', svc_id)
    
        response = yield self.rpc_send(svc_id, 'get', \
                                       ('baudrate', 'outputformat'), {})
        self.assertEqual(response['content'], {'baudrate' : 9600,
                                               'outputformat' : 0})
        response = yield self.rpc_send(svc_id, 'set', {'baudrate': 19200,
                                            'outputformat': 1}, {})
        self.assertEqual(response['content'], {'baudrate' : 19200,
                                               'outputformat' : 1})        
        response = yield self.rpc_send(svc_id, 'get', ('baudrate',
                                                       'outputformat'), {})
        self.assertEqual(response['content'], {'baudrate' : 19200,
                                               'outputformat' : 1})       
     
    """ 
        yield pu.send_message(self.receiver, '', svc_id, 'getLifecycleState', (), {})
     
        yield pu.send_message(self.receiver, '', svc_id, 'setLifecycleState', {})
    """

#    def receive(self, content, msg):
#        print 'in TestInstrumentAgent receive ', content, msg
#        instance.receive(content, msg)


def start():
    testset = TestInstrumentAgent()
    testset.setUp()
    testset.testGetSetSBE49Params()
    testset.tearDown()
