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

class TestInstrumentAgent(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        # Start an instrument agent
        processes = [
            {'name':'testSBE49IA','module':'ion.agents.instrumentagents.SBE49'},
        ]
        sup = yield self._spawn_processes(processes)
        svc_id = sup.get_child_id('testSBE49IA')

        # Start a client (for the RPC)
        self.IAClient = InstrumentAgentClient(proc=sup, target=svc_id)

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
         yield self.IAClient.rpc_send('getCapabilities', ())
        self.assert_(set(IAcommands) == set(content['commands']))
        self.assert_(set(IAparameters) == set(content['parameters']))

    @defer.inlineCallbacks
    def testGetSetSBE49Params(self):
        """
        Test the ability of the SBE49 driver to send and receive get, set,
        and other messages. Best called as RPC message pairs.
        """
        (content, headers, message) = \
            yield self.IAClient.rpc_send('get', ('baudrate','outputformat'))
        self.assertEqual(content, {'baudrate' : 9600,
                                   'outputformat' : 0})

        (content, headers, message) = \
            yield self.IAClient.rpc_send('set', {'baudrate': 19200,
                                                 'outputformat': 1})
        self.assertEqual(content, {'baudrate' : 19200,
                                   'outputformat' : 1})

        (content, headers, message) = \
            yield self.IAClient.rpc_send('get', ('baudrate', 'outputformat'))
        self.assertEqual(content, {'baudrate' : 19200,
                                   'outputformat' : 1})

        (content, headers, message) = \
            yield self.IAClient.rpc_send('setLifecycleState', 'undeveloped')
        self.assertEqual(content, 'undeveloped')

        (content, headers, message) = \
            yield self.IAClient.rpc_send('getLifecycleState', '')
        self.assertEqual(content, 'undeveloped')

        (content, headers, message) = \
            yield self.IAClient.rpc_send('setLifecycleState', 'developed')
        self.assertEqual(content, 'developed')

        (content, headers, message) = \
        yield self.IAClient.rpc_send('getLifecycleState', '')
        self.assertEqual(content, 'developed')
