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
from ion.agents.instrumentagents.SBE49 import instrumentCommands as IAcommands
from ion.agents.instrumentagents.SBE49 import instrumentParameters as IAparameters

import ion.util.procutils as pu

logging.basicConfig(level=logging.DEBUG)

class TestInstrumentAgent(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        IonTestCase.setUp(self)
        self.store = yield Store()
        yield self._startContainer()
        self.rpc = RpcClient()
        yield self.rpc.attach()
        
        # Setup a store and register a test instrument
        yield self.store.put('test_instrument', id)

        svc_mod = __import__('ion.agents.instrumentagents.SBE49', globals(),
                             locals(), ['SBE49InstrumentAgent'])

        # Spawn instance of a service
        self.svc_id = yield spawn(svc_mod)
    
        yield self.store.put('SBE49InstrumentAgent', self.svc_id)
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stopContainer()

    @defer.inlineCallbacks
    def testGetSBE49Capabilities(self):
        """
        Test the ability to gather capabilities from the SBE49 instrument
        capabilities
        """
        (content, headers, message) = yield self.rpc.rpc_send(self.svc_id, 'getCapabilities')
        self.assert_(set(IAcommands) == set(content['commands']))
        self.assert_(set(IAparameters) == set(content['parameters']))
        
    @defer.inlineCallbacks
    def testGetSetSBE49Params(self):
        """
        Test the ability of the SBE49 driver to send and receive get, set,
        and other messages. Best called as RPC message pairs.
        """
        (content, headers, message) = yield self.rpc.rpc_send(self.svc_id, 'get',
                                                              ('baudrate', 'outputformat'), {})
        self.assertEqual(content, {'baudrate' : 9600,
                                   'outputformat' : 0})
        (content, headers, message) = yield self.rpc.rpc_send(self.svc_id, 'set',
                                                              {'baudrate': 19200,
                                                               'outputformat': 1}, {})
        self.assertEqual(content, {'baudrate' : 19200,
                                   'outputformat' : 1})
        (content, headers, message) = yield self.rpc.rpc_send(self.svc_id, 'get',
                                                              ('baudrate',
                                                               'outputformat'), {})
        self.assertEqual(content, {'baudrate' : 19200,
                                   'outputformat' : 1})       
        
        (content, headers, message) = yield self.rpc.rpc_send(self.svc_id, 'setLifecycleState',
                                                              'undeveloped', {})
        self.assertEqual(content, 'undeveloped')       
        (content, headers, message) = yield self.rpc.rpc_send(self.svc_id, 'getLifecycleState',
                                                              '', {})
        self.assertEqual(content, 'undeveloped')       
        (content, headers, message) = yield self.rpc.rpc_send(self.svc_id, 'setLifecycleState',
                                                              'developed', {})
        self.assertEqual(content, 'developed')       
        (content, headers, message) = yield self.rpc.rpc_send(self.svc_id, 'getLifecycleState',
                                                              '', {})
        self.assertEqual(content, 'developed')       