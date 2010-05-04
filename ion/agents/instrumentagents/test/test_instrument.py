#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_instrument.py
@author Michael Meisinger
@author Stephen Pasco
@author Steve Foley
@todo Simplify spawn call?
@todo Clean out unnecessary imports
@todo Test registry of resource into registry
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

class TestInstrumentAgent(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        IonTestCase.setUp(self)
        self.store = yield Store()
        self._startContainer()
        self.rpc = RpcClient()
        yield self.rpc.attach()
        
    def tearDown(self):
        self._stopContainer()
    
    @defer.inlineCallbacks
    def testGetSBE49Capabilities(self):
        """
        Test the ability to gather capabilities from the SBE49 instrument
        capabilities
        """
    @defer.inlineCallbacks
    def testGetSetSBE49Params(self):
        """
        Test the ability of the SBE49 driver to send and receive get, set,
        and other messages. Best called as RPC message pairs.
        """
        yield self.store.put('test_instrument', id)
    
        svc_mod = __import__('ion.agents.instrumentagents.SBE49', globals(),
                             locals(), ['SBE49InstrumentAgent'])

        # Spawn instance of a service
        svc_id = yield spawn(svc_mod)
    
        yield self.store.put('SBE49InstrumentAgent', svc_id)
    
        response = yield self.rpc.rpc_send(svc_id, 'get', \
                                       ('baudrate', 'outputformat'), {})
        self.assertEqual(response['content'], {'baudrate' : 9600,
                                               'outputformat' : 0})
        response = yield self.rpc.rpc_send(svc_id, 'set', {'baudrate': 19200,
                                            'outputformat': 1}, {})
        self.assertEqual(response['content'], {'baudrate' : 19200,
                                               'outputformat' : 1})        
        response = yield self.rpc.rpc_send(svc_id, 'get', ('baudrate',
                                                       'outputformat'), {})
        self.assertEqual(response['content'], {'baudrate' : 19200,
                                               'outputformat' : 1})       
     
        response = yield self.rpc.rpc_send(svc_id, 'setLifecycleState',
                                       'undeveloped', {})
        self.assertEqual(response['content'], 'undeveloped')       
        response = yield self.rpc.rpc_send(svc_id, 'getLifecycleState', '', {})
        self.assertEqual(response['content'], 'undeveloped')       
        response = yield self.rpc.rpc_send(svc_id, 'setLifecycleState',
                                       'developed', {})
        self.assertEqual(response['content'], 'developed')       
        response = yield self.rpc.rpc_send(svc_id, 'getLifecycleState', '', {})
        self.assertEqual(response['content'], 'developed')       

"""
def start():
    testset = TestInstrumentAgent()
    testset.setUp()
    testset.testGetSetSBE49Params()
    testset.tearDown()
"""