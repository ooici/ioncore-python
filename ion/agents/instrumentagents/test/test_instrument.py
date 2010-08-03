#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_instrument.py
@author Michael Meisinger
@author Stephen Pasco
@author Steve Foley
@author Dave Everett
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
from ion.agents.instrumentagents.SBE49_constants import ci_commands as IACICommands
from ion.agents.instrumentagents.SBE49_constants import ci_parameters as IACIParameters
from ion.agents.instrumentagents.SBE49_constants import instrument_commands as IAInstCommands
from ion.agents.instrumentagents.SBE49_constants import instrument_parameters as IAInstParameters

from magnet.spawnable import Receiver
from magnet.spawnable import spawn
from ion.core.base_process import BaseProcess
from ion.services.dm.datapubsub import DataPubsubClient

import ion.util.procutils as pu
from subprocess import Popen, PIPE

import os

from twisted.trial import unittest

class TestInstrumentAgent(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        # Start the simulator
        logging.info("Starting instrument simulator.")

        """
        Construct the path to the instrument simulator, starting with the current
        working directory
        """
        cwd = os.getcwd()
        simPath = cwd.replace("_trial_temp", "ion/agents/instrumentagents/test/sim_SBE49.py")
        #logging.info("cwd: %s, simPath: %s" %(str(cwd), str(simPath)))
        self.simProc = Popen(simPath, stdout=PIPE)
        
        # Start an instrument agent and the pubsub service
        processes = [
            {'name':'testSBE49IA',
             'module':'ion.agents.instrumentagents.SBE49_IA',
             'class':'SBE49InstrumentAgent'},
            {'name':'resource_registry',
             'module':'ion.services.coi.resource_registry',
             'class':'ResourceRegistryService'},
            {'name':'data_pubsub',
             'module':'ion.services.dm.datapubsub',
             'class':'DataPubsubService'}
        ]
        self.sup = yield self._spawn_processes(processes)
        self.svc_id = yield self.sup.get_child_id('testSBE49IA')
        self.res_reg_id = yield self.sup.get_child_id('resource_registry')
        
        # Start a client (for the RPC)
        self.IAClient = InstrumentAgentClient(proc=self.sup, target=self.svc_id)
        # These might work, too, for starting a client
        #self.rrc = ResourceRegistryClient(proc=self.sup)
        #self.rrc = ResourceRegistryClient(target=self.sup.get_scoped_name('global', self.res_reg_id))
        
        yield self.IAClient.set_registry_client(str(self.res_reg_id))


    @defer.inlineCallbacks
    def tearDown(self):
        logging.info("Stopping instrument simulator.")
        self.simProc.terminate()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_get_SBE49_capabilities(self):
        """
        Test the ability to gather capabilities from the SBE49 instrument
        capabilities
        """
        result = yield self.IAClient.get_capabilities()
        self.assert_(list(IACICommands)
                     == list(result[IA.ci_commands]))
        self.assert_(list(IACIParameters)
                     == list(result[IA.ci_parameters]))
        self.assert_(list(IAInstCommands)
                     == list(result[IA.instrument_commands]))
        self.assert_(list(IAInstParameters)
                     == list(result[IA.instrument_parameters]))

    @defer.inlineCallbacks
    def test_get_set_SBE49_params(self):
        """
        Test the ability of the SBE49 driver to send and receive get, set,
        and other messages. Best called as RPC message pairs.
        """
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
        raise unittest.SkipTest('Needs instrument-specific registration')
        
        
    @defer.inlineCallbacks
    def test_lifecycle_states(self):
        """
        Test the resource lifecycle management
        """
        raise unittest.SkipTest('Still needs instrument-specific agent registry integration')
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
        Test to see if the status response is correct.  This is probably not the
        status that we really want to see from this command; in the SBE49
        instrument driver, this translates to the "ds" command; that might
        not be the intent of this command, but for now that's what it does.
        Can change it later; just want to get this stuff checked in.
        @todo Do we even need this function?
        """

        # Sleep for a while to allow simlator to get set up.
        yield pu.asleep(2)
        
        dpsc = DataPubsubClient(self.super)
        topic_name = yield dpsc.define_topic("topic1")
        
        dc1 = DataConsumer()
        dc1_id = yield dc1.spawn()
        yield dc1.attach(topic_name)

        response = yield self.IAClient.getStatus(['some_arg'])
        
        self.assert_(isinstance(response, dict))
        self.assertEqual(response['status'], "OK")
        self.assertEqual(response['value'], 'a-ok')
        
        # await the deliver of data message into consumer
        yield pu.asleep(2)
        
        # now check for response
        self.assertEqual(dc1.receive_cnt, 1)

        response = yield self.IAClient.disconnect(['some_arg'])
        
        
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
        
    def _get_datamsg(self, metadata, data):
        #metadata.update('timestamp':time.clock())
        return {'metadata':metadata, 'data':data}


class DataConsumer(BaseProcess):
    """
    A class for spawning as a separate process to consume the responses from
    the instrument.
    """

    @defer.inlineCallbacks
    def attach(self, topic_name):
        """
        Attach to the given topic name
        """
        yield self.init()
        self.dataReceiver = Receiver(__name__, topic_name)
        self.dataReceiver.handle(self.receive)
        self.dr_id = yield spawn(self.dataReceiver)

        self.receive_cnt = 0
        self.received_msg = []
        self.ondata = None

    @defer.inlineCallbacks
    def op_data(self, content, headers, msg):
        """
        Data has been received.  Increment the receive_cnt
        """
        self.receive_cnt += 1
        self.received_msg.append(content)


