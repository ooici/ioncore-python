#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_instrument.py
@author Michael Meisinger
@author Stephen Pasco
@author Steve Foley
@todo Test registry of resource into registry
"""

import logging
from twisted.internet import defer

from ion.services.coi.resource_registry import ResourceLCState as LCS
from ion.test.iontest import IonTestCase
from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.agents.instrumentagents.SBE49 import instrumentCommands as IAcommands
from ion.agents.instrumentagents.SBE49 import instrumentParameters as IAparameters

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
        result = yield self.IAClient.getCapabilities()
        self.assert_(set(IAcommands) == set(result['commands']))
        self.assert_(set(IAparameters) == set(result['parameters']))

    @defer.inlineCallbacks
    def testGetSetSBE49Params(self):
        """
        Test the ability of the SBE49 driver to send and receive get, set,
        and other messages. Best called as RPC message pairs.
        """
        response = yield self.IAClient.get(['baudrate','outputformat'])
        self.assertEqual(response, {'baudrate' : 9600, 'outputformat' : 0})

        response = yield self.IAClient.set({'baudrate': 19200,
                                            'outputformat': 1})
        self.assertEqual(response, {'baudrate' : 19200, 'outputformat' : 1})

        response = yield self.IAClient.get(['baudrate', 'outputformat'])
        self.assertEqual(response, {'baudrate' : 19200, 'outputformat' : 1})

        response = yield self.IAClient.setLifecycleState(LCS.RESLCS_INACTIVE)
        self.assertEqual(response, LCS.RESLCS_INACTIVE)

        response = yield self.IAClient.getLifecycleState()
        self.assertEqual(response, LCS.RESLCS_INACTIVE)

        response = yield self.IAClient.setLifecycleState(LCS.RESLCS_ACTIVE)
        self.assertEqual(response, LCS.RESLCS_ACTIVE)

        response = yield self.IAClient.getLifecycleState()
        self.assertEqual(response, LCS.RESLCS_ACTIVE)

    @defer.inlineCallbacks
    def testExecute(self):
        """
        Test the ability of the SBE49 driver to execute commands through the
        InstrumentAgentClient class
        """
        response = yield self.IAClient.execute(['start', 'pumpon', 'stop'])
        logging.debug("response is %s", response)
        
