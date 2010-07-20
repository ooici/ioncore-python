#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_instrument.py
@author Michael Meisinger
@author Stephen Pasco
@author Steve Foley
@todo Test registry of resource into registry
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.data.dataobject import LCStates as LCS
from ion.test.iontest import IonTestCase
from ion.agents.instrumentagents import instrument_agent as IA
from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.agents.instrumentagents.SBE49 import ci_commands as IACICommands
from ion.agents.instrumentagents.SBE49 import ci_parameters as IACIParameters
from ion.agents.instrumentagents.SBE49 import instrument_commands as IAInstCommands
from ion.agents.instrumentagents.SBE49 import instrument_parameters as IAInstParameters


class TestInstrumentAgent(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        # Start an instrument agent
        processes = [
            {'name':'testSBE49IA',
             'module':'ion.agents.instrumentagents.SBE49',
             'class':'SBE49InstrumentAgent'},
            {'name':'resource_registry',
             'module':'ion.services.coi.resource_registry',
             'class':'ResourceRegistryService'}
        ]
        self.sup = yield self._spawn_processes(processes)
        self.svc_id = yield self.sup.get_child_id('testSBE49IA')
        self.res_reg_id = yield self.sup.get_child_id('resource_registry')
        
        # Start a client (for the RPC)
        self.IAClient = InstrumentAgentClient(proc=self.sup, target=self.svc_id)
        # These might work, too, for starting a client
        #self.rrc = ResourceRegistryClient(proc=self.sup)
        #self.rrc = ResourceRegistryClient(target=self.sup.get_scoped_name('global', self.res_reg_id))
        
        yield self.IAClient.set_resource_registry_client(str(self.res_reg_id))


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_get_SBE49_capabilities(self):
        """
        Test the ability to gather capabilities from the SBE49 instrument
        capabilities
        """
        result = yield self.IAClient.get_capabilities()
        self.assert_(list(IACICommands) == result[IA.ci_commands])
        self.assert_(list(IACIParameters) == result[IA.ci_parameters])
        self.assert_(list(IAInstCommands) == result[IA.instrument_commands])
        self.assert_(list(IAInstParameters) == result[IA.instrument_parameters])

    @defer.inlineCallbacks
    def test_get_set_SBE49_params(self):
        """
        Test the ability of the SBE49 driver to send and receive get, set,
        and other messages. Best called as RPC message pairs.
        """
#        raise unittest.SkipTest('Needs Refactor of LifeCycle State and Resource Descriptions')

        response = yield self.IAClient.get_from_instrument(['baudrate','outputformat'])
        self.assert_(response['status'] == 'OK')
        self.assertEqual(response['baudrate'], 9600)
        self.assertEqual(response['outputformat'], 0)

        response = yield self.IAClient.set_to_instrument({'baudrate': 19200,
                                            'outputformat': 1})
        self.assert_(response['status'] == 'OK')
        self.assertEqual(response['baudrate'], 19200)
        self.assertEqual(response['outputformat'], 1)
        
        response = yield self.IAClient.get_from_instrument(['baudrate', 'outputformat'])
        self.assert_(response['status'] == 'OK')
        self.assertEqual(response['baudrate'], 19200)
        self.assertEqual(response['outputformat'], 1)
        
        # Try setting something bad
        response = yield self.IAClient.set_to_instrument({'baudrate': 19200,
                                            'badvalue': 1})
        self.assert_(response['status'] == 'ERROR')
        self.assert_('baudrate' not in response)

    @defer.inlineCallbacks
    def test_registration(self):
        """
        Tests the ability of an instrument agent to successfully register
        ifself with the resource registry.
        """
        # Setup a registry client that ties to the same place and query for it
        # ...problem is we need the id that it registered with
        #rid = yield self.IAClient.get(resource_id)
        #rd3 = yield self.rrc.get_resource(rid)
        raise unittest.SkipTest('Needs a working registration plan')

        
        
    @defer.inlineCallbacks
    def test_lifecycle_states(self):
        """
        Test the resource lifecycle management
        """
        raise unittest.SkipTest('Still needs resource registry integration')
        """
        response = yield self.IAClient.set_lifecycle_state(LCS.inactive)
        self.assertEqual(response, LCS.inactive)

        response = yield self.IAClient.get_lifecycle_state()
        self.assertEqual(response, LCS.inactive)
        self.assertNotEqual(response, LCS.active)

        response = yield self.IAClient.set_lifecycle_state(active)
        self.assertEqual(response, active)

        response = yield self.IAClient.get_lifecycle_state()
        self.assertEqual(response, active)

        response = yield self.IAClient.set_lifecycle_state(LCS('foobar'))
        self.assertEqual(response, active)
        """

    @defer.inlineCallbacks
    def test_execute(self):
        """
        Test the ability of the SBE49 driver to execute commands through the
        InstrumentAgentClient class
        """
        response = yield self.IAClient.execute_instrument({'start':['now', 1],
                                                           'stop':[]})
        self.assert_(isinstance(response, dict))
        self.assert_('status' in response.keys())
        self.assertEqual(response['status'], 'OK')
        self.assert_('start' in response['value'])
        self.assert_('stop' in response['value'])
        self.assert_(response['status'] == 'OK')

        response = yield self.IAClient.execute_instrument({'badcommand':['now', '1']})
    
        self.assert_(isinstance(response, dict))
        self.assertEqual(response['status'], 'ERROR') 
                
        response = yield self.IAClient.execute_instrument({})
        self.assert_(isinstance(response, dict))
        self.assertEqual(response['status'], 'ERROR')
        
        

        
    @defer.inlineCallbacks
    def test_status(self):
        """
        Test to see if the status response is correct
        @todo Do we even need this function?
        """
        response = yield self.IAClient.get_status(['some_arg'])
        logging.debug("*** testStatus response: %s", response)
        self.assert_(isinstance(response, dict))
        self.assertEqual(response['status'], "OK")
        self.assertEqual(response['value'], 'a-ok')
        
    @defer.inlineCallbacks
    def test_translator(self):
        """
        Test to see if the translator function is coming back cleanly
        @todo make this not a stub when we can pass functions through AMQP
        """
        raise unittest.SkipTest('Needs Refactor of LifeCycle State and Resource Descriptions')

        yield
        #xlateFn = yield self.IAClient.getTranslator()
        #self.assert_(inspect.isroutine(xlateFn))
        #self.assert_(xlateFn('foo') == 'foo')