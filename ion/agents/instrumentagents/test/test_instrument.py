#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_instrument.py
@author Michael Meisinger
@author Stephen Pasco
@author Steve Foley
@todo Test registry of resource into registry
"""

import logging
import inspect
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
        self.assert_(response['status'] == 'OK')
        self.assertEqual(response['baudrate'], 9600)
        self.assertEqual(response['outputformat'], 0)

        response = yield self.IAClient.set({'baudrate': 19200,
                                            'outputformat': 1})
        self.assert_(response['status'] == 'OK')
        self.assertEqual(response['baudrate'], 19200)
        self.assertEqual(response['outputformat'], 1)
        
        response = yield self.IAClient.get(['baudrate', 'outputformat'])
        self.assert_(response['status'] == 'OK')
        self.assertEqual(response['baudrate'], 19200)
        self.assertEqual(response['outputformat'], 1)
        
        # Try setting something bad
        response = yield self.IAClient.set({'baudrate': 19200,
                                            'badvalue': 1})
        self.assert_(response['status'] == 'ERROR')
        self.assert_('baudrate' not in response)
        
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
        response = yield self.IAClient.execute(['start', 'badcommand', 'stop'])
        self.assert_(isinstance(response, dict))
        self.assert_('badcommand' in response)
        self.assert_('start' in response)
        self.assert_('stop' in response)
        self.assert_(isinstance(response['start'], dict))
        self.assert_(isinstance(response['stop'], dict))
        self.assert_(isinstance(response['badcommand'], dict))
        self.assert_(response['start']['status'] == 'OK')
        self.assert_(response['stop']['status'] == 'OK')
        self.assert_(response['badcommand']['status'] == 'ERROR')
        
    @defer.inlineCallbacks
    def testStatus(self):
        """
        Test to see if the status response is correct
        """
        response = yield self.IAClient.getStatus(['some_arg'])
        logging.debug("testStatus response: %s", response)
        self.assert_(isinstance(response, dict))
        self.assertEqual(response['status'], "OK")
        self.assertEqual(response['result'], 'a-ok')
        
    @defer.inlineCallbacks
    def testTranslator(self):
        """
        Test to see if the translator function is coming back cleanly
        @todo make this not a stub when we can pass functions through AMQP
        """
        yield
        #xlateFn = yield self.IAClient.getTranslator()
        #self.assert_(inspect.isroutine(xlateFn))
        #self.assert_(xlateFn('foo') == 'foo')