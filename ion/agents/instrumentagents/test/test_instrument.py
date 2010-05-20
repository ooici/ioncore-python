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

from ion.data.store import Store
from ion.services.base_service import BaseServiceClient
from ion.test.iontest import IonTestCase
from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.agents.instrumentagents.SBE49 import instrumentCommands as IAcommands
from ion.agents.instrumentagents.SBE49 import instrumentParameters as IAparameters
from ion.agents.instrumentagents.SBE49 import SBE49InstrumentAgent as SBE49IA

import ion.util.procutils as pu

logging.basicConfig(level=logging.DEBUG)

class TestInstrumentAgent(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        IonTestCase.setUp(self)
        self.store = yield Store()
        yield self._start_container()
        
        # Start an instrument agent and client (for the RPC)
        self.instrumentAgent = yield SBE49IA()
        self.svc_id = yield self.instrumentAgent.spawn()
        self.IAClient = InstrumentAgentClient()
        self.client_id = yield self.IAClient.spawn()

        yield self.store.put('testSBE49InstrumentAgent', self.svc_id)
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def testGetSBE49Capabilities(self):
        """
        Test the ability to gather capabilities from the SBE49 instrument
        capabilities
        """
        (content, headers, message) = \
         yield self.IAClient.rpc_send(self.svc_id,
                                      'getCapabilities',
                                      (), {})
        self.assert_(set(IAcommands) == set(content['commands']))
        self.assert_(set(IAparameters) == set(content['parameters']))
        
    @defer.inlineCallbacks
    def testGetSetSBE49Params(self):
        """
        Test the ability of the SBE49 driver to send and receive get, set,
        and other messages. Best called as RPC message pairs.
        """
        (content, headers, message) = \
         yield self.IAClient.rpc_send(self.svc_id, 'get', ('baudrate',
                                                           'outputformat'), {})
        self.assertEqual(content, {'baudrate' : 9600,
                                   'outputformat' : 0})
        (content, headers, message) = \
         yield self.IAClient.rpc_send(self.svc_id, 'set', {'baudrate': 19200,
                                                           'outputformat': 1}, {})
        self.assertEqual(content, {'baudrate' : 19200,
                                   'outputformat' : 1})
        (content, headers, message) = \
         yield self.IAClient.rpc_send(self.svc_id, 'get', ('baudrate',
                                                           'outputformat'), {})
        self.assertEqual(content, {'baudrate' : 19200, 'outputformat' : 1})       
        
        (content, headers, message) = \
         yield self.IAClient.rpc_send(self.svc_id, 'setLifecycleState', 
                                      'undeveloped', {})
        self.assertEqual(content, 'undeveloped')       
        (content, headers, message) = \
         yield self.IAClient.rpc_send(self.svc_id, 'getLifecycleState', '', {})
        self.assertEqual(content, 'undeveloped')       
        (content, headers, message) = \
         yield self.IAClient.rpc_send(self.svc_id, 'setLifecycleState',
                                      'developed', {})
        self.assertEqual(content, 'developed')       
        (content, headers, message) = \
         yield self.IAClient.rpc_send(self.svc_id, 'getLifecycleState', '', {})
        self.assertEqual(content, 'developed')       