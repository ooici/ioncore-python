#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_SBE37_agent.py
@brief Test cases for the InstrumentAgent and InstrumentAgentClient classes
    using a live SBE37 driver.
@author Edward Hunter
"""

import uuid
import re
import os

from twisted.internet import defer
from ion.test.iontest import IonTestCase
from twisted.trial import unittest

import ion.util.ionlog
import ion.util.procutils as pu
from ion.core.process.process import Process
from ion.core.exception import ReceivedError
from ion.services.dm.distribution.events import DataBlockEventSubscriber
from ion.services.dm.distribution.events import InfoLoggingEventSubscriber
from ion.services.dm.distribution.events \
    import BusinessStateChangeSubscriber
import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.agents.instrumentagents.instrument_constants import AgentCommand
from ion.agents.instrumentagents.instrument_constants import AgentParameter
from ion.agents.instrumentagents.instrument_constants import AgentEvent
from ion.agents.instrumentagents.instrument_constants import AgentStatus
from ion.agents.instrumentagents.instrument_constants import AgentState
from ion.agents.instrumentagents.instrument_constants import DriverChannel
from ion.agents.instrumentagents.instrument_constants import DriverCommand
from ion.agents.instrumentagents.instrument_constants import DriverParameter
from ion.agents.instrumentagents.instrument_constants import InstErrorCode
from ion.agents.instrumentagents.instrument_constants \
    import InstrumentCapability
from ion.agents.instrumentagents.instrument_constants import MetadataParameter
from ion.agents.instrumentagents.SBE37_driver import SBE37Channel
from ion.agents.instrumentagents.SBE37_driver import SBE37Command
from ion.agents.instrumentagents.SBE37_driver import SBE37Parameter
from ion.agents.instrumentagents.SBE37_driver import SBE37MetadataParameter
from ion.agents.instrumentagents.SBE37_driver import SBE37Status


log = ion.util.ionlog.getLogger(__name__)

#############################################################################
#
# Methods to isolate testing to allowed machines.
# These tests will be skipped or disabled where not explicitly allowed.
#
############################################################################

"""
This method is deprecated in favor of an environment variable flag below.

List of mac addresses for machines which should run these tests. If no
mac address of a NIC on the machine running the tests matches one in this
list, the tests are skipped. This is to prevent the trial robot from
commanding the instrument hardware, forcing these tests to be run
intentionally. Add the mac address of your development machine as
returned by ifconfig to cause the tests to run for you locally.
"""
"""
allowed_mac_addr_list = [
    '00:26:bb:19:83:33'         # Edward's Macbook
    ]

mac_addr_pattern = r'\b\w\w[:\-]\w\w[:\-]\w\w[:\-]\w\w[:\-]\w\w[:\-]\w\w\b'
mac_addr_re = re.compile(mac_addr_pattern,re.MULTILINE)
mac_addr_list = mac_addr_re.findall(os.popen('ifconfig').read())
RUN_TESTS = any([addr in allowed_mac_addr_list for addr in mac_addr_list])
"""

"""
Set RUN_TESTS flag if the correct environment variable has been assigned.
Export this variable to 'available' in your shell startup script to enable
hardware tests to run.
"""
RUN_TESTS = True if os.environ.get('LIVE_SBE37_HARDWARE',None) \
    == 'available' else False


# It is useful to be able to easily turn tests on and off
# during development. Also this will ensure tests do not run
# automatically. 
SKIP_TESTS = [
    #'test_execute_instrument',
    #'test_state_transitions',
    #'test_get_capabilities',
    'dummy'
]    


PRINT_PUBLICATIONS = (True, False)[0]

class TestSBE37Agent(IonTestCase):

    # Increase the timeout so we can handle longer instrument interactions.
    timeout = 300
    

    @defer.inlineCallbacks
    def setUp(self):
        
        
        yield self._start_container()


        # Driver and agent configuration. Configuration data will ultimatly be
        # accessed via some persistance mechanism: platform filesystem
        # or a device registry. For now, we pass all configuration data
        # that would be read this way as process arguments.
        sbe_host = '137.110.112.119'
        sbe_port = 4001    
        driver_config = {
            'ipport':sbe_port,
            'ipaddr':sbe_host
        }
        agent_config = {}

        # Process description for the SBE37 driver.
        driver_desc = {
            'name':'SBE37_driver',
            'module':'ion.agents.instrumentagents.SBE37_driver',
            'class':'SBE37Driver',
            'spawnargs':{'config':driver_config}
        }

        # Process description for the SBE37 driver client.
        driver_client_desc = {
            'name':'SBE37_client',
            'module':'ion.agents.instrumentagents.SBE37_driver',
            'class':'SBE37DriverClient',
            'spawnargs':{}
        }

        # Spawnargs for the instrument agent.
        spawnargs = {
            'driver-desc':driver_desc,
            'client-desc':driver_client_desc,
            'driver-config':driver_config,
            'agent-config':agent_config
        }

        # Process description for the instrument agent.
        agent_desc = {
            'name':'instrument_agent',
            'module':'ion.agents.instrumentagents.instrument_agent',
            'class':'InstrumentAgent',
            'spawnargs':spawnargs
        }

        # Processes for the tests.
        processes = [
            agent_desc
        ]

        # Spawn agent and driver, create agent client.
        self.sup = yield self._spawn_processes(processes)
        self.svc_id = yield self.sup.get_child_id('instrument_agent')
        self.ia_client = instrument_agent.InstrumentAgentClient(proc=self.sup,
                                                            target=self.svc_id)        

        # Setup a subscriber to a data event topic
        class TestDataSubscriber(DataBlockEventSubscriber):
            def __init__(self, *args, **kwargs):
                self.msgs = []
                DataBlockEventSubscriber.__init__(self, *args, **kwargs)
                print 'listening for data at ' + kwargs.get('origin','none') 

            def ondata(self, data):
                content = data['content'];
                if PRINT_PUBLICATIONS:
                    print 'data subscriber ondata:'
                    print content.additional_data.data_block

        # origin format = transducer.agent_proc_id
        # CHANNEL_INSTRUMENT.dyn137-110-115-127_ucsd_edu_913.5
        origin_str = DriverChannel.INSTRUMENT + '.' + str(self.svc_id)
        datasub = TestDataSubscriber(origin=origin_str,process=self.sup)
        yield datasub.initialize()
        yield datasub.activate()

        # Setup a subscriber to agent errors, transactions, config changes.
        class TestInfoSubscriber(InfoLoggingEventSubscriber):
            def __init__(self, *args, **kwargs):
                self.msgs = []
                InfoLoggingEventSubscriber.__init__(self, *args, **kwargs)                
                print 'listening for info at ' + kwargs.get('origin','none')          
                
            def ondata(self, data):
                content = data['content'];
                if PRINT_PUBLICATIONS:
                    print 'logging subscriber ondata:'
                    print content.description
                    #print content.additional_data.data_block

        # origin format = transducer.agent_proc_id
        # CHANNEL_INSTRUMENT.dyn137-110-115-127_ucsd_edu_913.5
        origin_str = 'agent.' + str(self.svc_id)
        infosub = TestInfoSubscriber(origin=origin_str,process=self.sup)
        yield infosub.initialize()
        yield infosub.activate()

        # Setup a subscriber to agent state changes.
        class TestStateSubscriber(BusinessStateChangeSubscriber):
            def __init__(self, *args, **kwargs):
                self.msgs = []
                BusinessStateChangeSubscriber.__init__(self, *args, **kwargs)
                print 'listening for state at ' + kwargs.get('origin','none') 

            def ondata(self, data):
                content = data['content'];
                if PRINT_PUBLICATIONS:
                    print 'state subscriber ondata:'
                    print content.description
                    #print content.additional_data.data_block

        # origin format = transducer.agent_proc_id
        # CHANNEL_INSTRUMENT.dyn137-110-115-127_ucsd_edu_913.5
        origin_str = 'agent.' + str(self.svc_id)
        statesub = TestStateSubscriber(origin=origin_str,process=self.sup)
        yield statesub.initialize()
        yield statesub.activate()


    @defer.inlineCallbacks
    def tearDown(self):
        
        pu.asleep(1)
        yield self._stop_container()
        

    @defer.inlineCallbacks
    def test_state_transitions(self):
        """
        Test cases for exectuing device commands through the instrument
        agent.
        """
        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_state_transitions' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        # Check agent state upon creation. No transaction needed for
        # get operation.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.UNINITIALIZED)

        # Check that the driver and client descriptions were set by
        # spawnargs, and save them for later restore.
        
        
        # Begin an explicit transaciton.
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        tid = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(tid),str)
        self.assertEqual(len(tid),36)
        
        # Initialize with a bad process desc. value. This should fail
        # and leave us in the uninitialized state with null driver and client.
        
        
        # Initialize with a bad client desc. value. This should fail and
        # leave us in the uninitialized state with null driver and client.
        
        
        # Restore the good process and client desc. values.
        

        # Initialize the agent to bring up the driver and client.
        cmd = [AgentCommand.TRANSITION,AgentEvent.INITIALIZE]
        reply = yield self.ia_client.execute_observatory(cmd,tid) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.INACTIVE)

        # Connect to the driver.
        cmd = [AgentCommand.TRANSITION,AgentEvent.GO_ACTIVE]
        reply = yield self.ia_client.execute_observatory(cmd,tid) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.IDLE)
        
        # Enter observatory mode.
        cmd = [AgentCommand.TRANSITION,AgentEvent.RUN]
        reply = yield self.ia_client.execute_observatory(cmd,tid) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))        
    
        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.OBSERVATORY_MODE)
        
        """
        # Discnnect from the driver.
        cmd = [AgentCommand.TRANSITION,AgentEvent.GO_INACTIVE]
        reply = yield self.ia_client.execute_observatory(cmd,tid) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))
        
        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.INACTIVE)
        """
        
        # Reset the agent to disconnect and bring down the driver and client.
        cmd = [AgentCommand.TRANSITION,AgentEvent.RESET]
        reply = yield self.ia_client.execute_observatory(cmd,tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.UNINITIALIZED)        

        # End the transaction.
        reply = yield self.ia_client.end_transaction(tid)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))
        
        
    @defer.inlineCallbacks
    def test_execute_instrument(self):
        """
        Test cases for exectuing device commands through the instrument
        agent.
        """
        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_execute_instrument' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')
                
        # Check agent state upon creation. No transaction needed for
        # get operation.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.UNINITIALIZED)

        # Begin an explicit transaciton.
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        tid = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(tid),str)
        self.assertEqual(len(tid),36)
        
        # Initialize the agent to bring up the driver and client.
        cmd = [AgentCommand.TRANSITION,AgentEvent.INITIALIZE]
        reply = yield self.ia_client.execute_observatory(cmd,tid) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.INACTIVE)

        # Connect to the driver.
        cmd = [AgentCommand.TRANSITION,AgentEvent.GO_ACTIVE]
        reply = yield self.ia_client.execute_observatory(cmd,tid) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.IDLE)
        
        # Enter observatory mode.
        cmd = [AgentCommand.TRANSITION,AgentEvent.RUN]
        reply = yield self.ia_client.execute_observatory(cmd,tid) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))        
    
        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.OBSERVATORY_MODE)
        
        # Get driver parameters.
        params = [(DriverChannel.ALL,DriverParameter.ALL)]
        reply = yield self.ia_client.get_device(params,tid)
        success = reply['success']
        result = reply['result']

        # Strip off individual success vals to create a set params to
        # restore original config later.
        orig_config = dict(map(lambda x : (x[0],x[1][1]),result.items()))

        self.assert_(InstErrorCode.is_ok(success))

        # Set a few parameters. This will test the device set functions
        # and set up the driver for sampling commands. 
        params = {}
        params[(DriverChannel.INSTRUMENT,'NAVG')] = 1
        params[(DriverChannel.INSTRUMENT,'INTERVAL')] = 5
        params[(DriverChannel.INSTRUMENT,'OUTPUTSV')] = True
        params[(DriverChannel.INSTRUMENT,'OUTPUTSAL')] = True
        params[(DriverChannel.INSTRUMENT,'TXREALTIME')] = True
        params[(DriverChannel.INSTRUMENT,'STORETIME')] = True
        
        reply = yield self.ia_client.set_device(params,tid)
        success = reply['success']
        result = reply['result']
        setparams = params
        
        self.assert_(InstErrorCode.is_ok(success))

        # Verify the set changes were made.
        params = [(DriverChannel.ALL,DriverParameter.ALL)]
        reply = yield self.ia_client.get_device(params,tid)
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))

        self.assertEqual(setparams[(DriverChannel.INSTRUMENT,'NAVG')],
                         result[(DriverChannel.INSTRUMENT,'NAVG')][1])
        self.assertEqual(setparams[(DriverChannel.INSTRUMENT,'INTERVAL')],
                         result[(DriverChannel.INSTRUMENT,'INTERVAL')][1])
        self.assertEqual(setparams[(DriverChannel.INSTRUMENT,'OUTPUTSV')],
                         result[(DriverChannel.INSTRUMENT,'OUTPUTSV')][1])
        self.assertEqual(setparams[(DriverChannel.INSTRUMENT,'OUTPUTSAL')],
                         result[(DriverChannel.INSTRUMENT,'OUTPUTSAL')][1])
        self.assertEqual(setparams[(DriverChannel.INSTRUMENT,'TXREALTIME')],
                         result[(DriverChannel.INSTRUMENT,'TXREALTIME')][1])
        self.assertEqual(setparams[(DriverChannel.INSTRUMENT,'STORETIME')],
                         result[(DriverChannel.INSTRUMENT,'STORETIME')][1])
        
        #print 'acquisition parameters successfully set'
        
        # Acquire sample.
        chans = [DriverChannel.INSTRUMENT]
        cmd = [DriverCommand.ACQUIRE_SAMPLE]
        reply = yield self.ia_client.execute_device(chans,cmd,tid)
        success = reply['success']
        result = reply['result']        

        #print 'acquisition result'
        #print result

        self.assert_(InstErrorCode.is_ok(success))
        self.assert_(len(result)==1)
        self.assertIsInstance(result[0].get('temperature',None),float)
        self.assertIsInstance(result[0].get('salinity',None),float)
        self.assertIsInstance(result[0].get('sound_velocity',None),float)
        self.assertIsInstance(result[0].get('pressure',None),float)
        self.assertIsInstance(result[0].get('conductivity',None),float)
        self.assertIsInstance(result[0].get('device_time',None),str)
        self.assertIsInstance(result[0].get('driver_time',None),str)
        
        # Start autosampling.
        chans = [DriverChannel.INSTRUMENT]
        cmd = [DriverCommand.START_AUTO_SAMPLING]
        reply = yield self.ia_client.execute_device(chans,cmd,tid)
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))

        #print 'autosampling started'
        
        # Wait for a few samples to arrive.
        yield pu.asleep(45)
        
        # Stop autosampling.
        chans = [DriverChannel.INSTRUMENT]
        cmd = [DriverCommand.STOP_AUTO_SAMPLING,'GETDATA']

        reply = yield self.ia_client.execute_device(chans,cmd,tid)
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        for sample in result:
            self.assertIsInstance(sample.get('temperature'),float)
            self.assertIsInstance(sample.get('salinity'),float)
            self.assertIsInstance(sample.get('pressure',None),float)
            self.assertIsInstance(sample.get('sound_velocity',None),float)
            self.assertIsInstance(sample.get('conductivity',None),float)
            self.assertIsInstance(sample.get('device_time',None),str)
            self.assertIsInstance(sample.get('driver_time',None),str)
       
        """
        while True:
            reply = yield self.ia_client.execute_device(chans,cmd,tid)
            success = reply['success']
            result = reply['result']
            
            if InstErrorCode.is_ok(success):
                break
            
            #elif success == InstErrorCode.TIMEOUT:
            elif InstErrorCode.is_equal(success,InstErrorCode.TIMEOUT):
                pass
            
            else:
                self.fail('Stop autosample failed with error: '+str(success))
            
        #print 'autosample result'
        #print result
        
        self.assert_(InstErrorCode.is_ok(success))
        for sample in result:
            self.assertIsInstance(sample.get('temperature'),float)
            self.assertIsInstance(sample.get('salinity'),float)
            self.assertIsInstance(sample.get('pressure',None),float)
            self.assertIsInstance(sample.get('sound_velocity',None),float)
            self.assertIsInstance(sample.get('conductivity',None),float)
            self.assertIsInstance(sample.get('device_time',None),str)
            self.assertIsInstance(sample.get('driver_time',None),str)
        """
        
        # Restore original configuration.
        reply = yield self.ia_client.set_device(orig_config,tid)
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))

        # Verify the original configuration was restored.    
        params = [(DriverChannel.ALL,DriverParameter.ALL)]
        reply = yield self.ia_client.get_device(params,tid)
        success = reply['success']
        result = reply['result']

        # Strip off individual success vals to create a set params to
        # restore original config later.
        final_config = dict(map(lambda x : (x[0],x[1][1]),result.items()))

        self.assert_(InstErrorCode.is_ok(success))
        for (key,val) in orig_config.iteritems():
            if isinstance(val,float):
                self.assertAlmostEqual(val,final_config[key],4)
            else:
                self.assertEqual(val,final_config[key])
                
        # Reset the agent to disconnect and bring down the driver and client.
        cmd = [AgentCommand.TRANSITION,AgentEvent.RESET]
        reply = yield self.ia_client.execute_observatory(cmd,tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.UNINITIALIZED)        

        # End the transaction.
        reply = yield self.ia_client.end_transaction(tid)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))



    @defer.inlineCallbacks
    def test_get_capabilities(self):
        """
        Test cases for querying the device and observatory capabilities.
        """
        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_get_capabilities' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')


        # Check agent state upon creation. No transaction needed for
        # get operation.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.UNINITIALIZED)

        # Begin an explicit transaciton.
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        tid = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(tid),str)
        self.assertEqual(len(tid),36)
        
        # Initialize the agent to bring up the driver and client.
        cmd = [AgentCommand.TRANSITION,AgentEvent.INITIALIZE]
        reply = yield self.ia_client.execute_observatory(cmd,tid) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.INACTIVE)

        #
        params = [InstrumentCapability.ALL]
        reply = yield self.ia_client.get_capabilities(params,tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))
        
        self.assertEqual(list(result[InstrumentCapability.\
            DEVICE_CHANNELS][1]).sort(),SBE37Channel.list().sort())
        self.assertEqual(list(result[InstrumentCapability.\
            DEVICE_COMMANDS][1]).sort(),SBE37Command.list().sort())
        self.assertEqual(list(result[InstrumentCapability.\
            DEVICE_METADATA][1]).sort(),SBE37MetadataParameter.list().sort())
        self.assertEqual(list(result[InstrumentCapability.\
            DEVICE_PARAMS][1]).sort(),SBE37Parameter.list().sort())
        self.assertEqual(list(result[InstrumentCapability.\
            DEVICE_STATUSES][1]).sort(),SBE37Status.list().sort())
        
        self.assertEqual(list(result[InstrumentCapability.\
            OBSERVATORY_COMMANDS][1]).sort(),AgentCommand.list().sort())
        self.assertEqual(list(result[InstrumentCapability.\
            OBSERVATORY_METADATA][1]).sort(),MetadataParameter.list().sort())
        self.assertEqual(list(result[InstrumentCapability.\
            OBSERVATORY_PARAMS][1]).sort(),AgentParameter.list().sort())
        self.assertEqual(list(result[InstrumentCapability.\
            OBSERVATORY_STATUSES][1]).sort(),AgentStatus.list().sort())

        # Reset the agent to disconnect and bring down the driver and client.
        cmd = [AgentCommand.TRANSITION,AgentEvent.RESET]
        reply = yield self.ia_client.execute_observatory(cmd,tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        # Check agent state.
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status(params,tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok(success))        
        self.assert_(agent_state == AgentState.UNINITIALIZED)        

        # End the transaction.
        reply = yield self.ia_client.end_transaction(tid)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))

