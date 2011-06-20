#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_NMEA0183_agent.py
@brief Test cases for the InstrumentAgent and InstrumentAgentClient classes using
 an NMEA0813 driver against a live or simulated instrument.
@author Alon Yaari
"""

import uuid
import re
import os

from twisted.internet import defer
from ion.agents.instrumentagents.agent_shell_api import driver_config
from ion.test.iontest import IonTestCase
from twisted.trial import unittest

import ion.util.ionlog
import ion.util.procutils as pu
from ion.core.exception import ReceivedError
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
from ion.agents.instrumentagents.instrument_constants import InstrumentCapability
from ion.agents.instrumentagents.instrument_constants import MetadataParameter
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceChannel
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceCommand
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceParam
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceMetadataParameter
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceStatus

from ion.agents.instrumentagents.simulators.sim_NMEA0183 import NMEA0183Simulator as SIM


log = ion.util.ionlog.getLogger(__name__)

"""
    These tests requires that a simulator (or real NMEA GPS device!) is attached to:
            /dev/slave
"""

class TestNMEA0183Agent (IonTestCase):

    # Increase the timeout so we can handle longer instrument interactions.
    timeout = 10


    @defer.inlineCallbacks
    def setUp (self):

        log.info("||||||| TestNMEA0183Agent.setUp")

        yield self._start_container()

        # Driver and agent configuration. Configuration data will ultimately be accessed via
        # some persistence mechanism: platform filesystem or a device registry.
        # For now, we pass all configuration data that would be read this way as process arguments.
        device_port             = "/dev/slave"
        device_baud             = 19200
        device_bytesize         = 8
        device_parity           = 'N'
        device_stopbits         = 1
        device_timeout          = 0
        device_xonxoff          = 0
        device_rtscts           = 0

        driver_config       = { 'port':         device_port,
                                'baudrate':     device_baud,
                                'bytesize':     device_bytesize,
                                'parity':       device_parity,
                                'stopbits':     device_stopbits,
                                'timeout':      device_timeout,
                                'xonxoff':      device_xonxoff,
                                'rtscts':       device_rtscts }
        agent_config        = {}
        
        # Process description for the instrument driver.
        driver_desc         = { 'name':         'NMEA0183_Driver',
                                'module':       'ion.agents.instrumentagents.driver_NMEA0183',
                                'class':        'NMEADeviceDriver',
                                'spawnargs':  { 'config': driver_config } }

        # Process description for the instrument driver client.
        driver_client_desc  = { 'name':         'NMEA0813_Client',
                                'module':       'ion.agents.instrumentagents.driver_NMEA0183',
                                'class':        'NMEADeviceDriverClient',
                                'spawnargs':    {} }

        # Spawnargs for the instrument agent.
        spawnargs           = { 'driver-desc':  driver_desc,
                                'client-desc':  driver_client_desc,
                                'driver-config':driver_config,
                                'agent-config': agent_config }

        # Process description for the instrument agent.
        agent_desc          = { 'name':         'instrument_agent',
                                'module':       'ion.agents.instrumentagents.instrument_agent',
                                'class':        'InstrumentAgent',
                                'spawnargs':    spawnargs }

        # Processes for the tests.
        processes           = [ agent_desc ]
        
        # Spawn agent and driver, create agent client.
        self.sup            = yield self._spawn_processes (processes)
        self.svc_id         = yield self.sup.get_child_id ('instrument_agent')
        self.ia_client      = instrument_agent.InstrumentAgentClient (proc = self.sup, target = self.svc_id)


    @defer.inlineCallbacks
    def tearDown (self):
        log.info("||||||| TestNMEA0183Agent.tearDown")

        pu.asleep(1)
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_state_transitions (self):
        """
        Test cases for executing device commands through the instrument agent.
        """

        log.info("||||||| TestNMEA0183Agent.test_state_transitions\n")

        # Check agent state upon creation. No transaction needed for get operation.
        params                          = [AgentStatus.AGENT_STATE]
        reply                           = yield self.ia_client.get_observatory_status (params)
        success                         = reply['success']
        result                          = reply['result']
        agent_state                     = result[AgentStatus.AGENT_STATE][1]
        self.assert_                    (InstErrorCode.is_ok (success))
        self.assert_                    (agent_state == AgentState.UNINITIALIZED)

        # Check that the driver and client descriptions were set by spawnargs, and save them for later restore.

        # Begin an explicit transaciton.
        reply                           = yield self.ia_client.start_transaction (0)
        success                         = reply['success']
        tid                             = reply['transaction_id']
        self.assert_                    (InstErrorCode.is_ok (success))
        self.assertEqual                (type (tid), str)
        self.assertEqual                (len (tid), 36)

        # Initialize with a bad process desc. value.
        # This should fail and leave us in the uninitialized state with null driver and client.

        # Initialize with a bad client desc. value.
        # This should fail and leave us in the uninitialized state with null driver and client.

        # Restore the good process and client desc. values.

        # Initialize the agent to bring up the driver and client.
        cmd                             = [AgentCommand.TRANSITION, AgentEvent.INITIALIZE]
        reply                           = yield self.ia_client.execute_observatory (cmd, tid)
        success                         = reply['success']
        result                          = reply['result']
        self.assert_                    (InstErrorCode.is_ok (success))

        # Check agent state.
        params                          = [AgentStatus.AGENT_STATE]
        reply                           = yield self.ia_client.get_observatory_status (params, tid)
        success                         = reply['success']
        result                          = reply['result']
        agent_state                     = result[AgentStatus.AGENT_STATE][1]
        self.assert_                    (InstErrorCode.is_ok (success))
        self.assert_                    (agent_state == AgentState.INACTIVE)

        # Connect to the driver.
        cmd                             = [AgentCommand.TRANSITION,AgentEvent.GO_ACTIVE]
        reply                           = yield self.ia_client.execute_observatory (cmd, tid)
        success                         = reply['success']
        result                          = reply['result']
        self.assert_                    (InstErrorCode.is_ok (success))

        # Check agent state.
        params                          = [AgentStatus.AGENT_STATE]
        reply                           = yield self.ia_client.get_observatory_status (params, tid)
        success                         = reply['success']
        result                          = reply['result']
        agent_state                     = result[AgentStatus.AGENT_STATE][1]
        self.assert_                    (InstErrorCode.is_ok (success))
        self.assert_                    (agent_state == AgentState.IDLE)
        
        # Enter observatory mode.
        cmd                             = [AgentCommand.TRANSITION, AgentEvent.RUN]
        reply                           = yield self.ia_client.execute_observatory (cmd, tid)
        success                         = reply['success']
        result                          = reply['result']
        self.assert_                    (InstErrorCode.is_ok (success))
    
        # Check agent state.
        params                          = [AgentStatus.AGENT_STATE]
        reply                           = yield self.ia_client.get_observatory_status (params, tid)
        success                         = reply['success']
        result                          = reply['result']
        agent_state                     = result[AgentStatus.AGENT_STATE][1]
        self.assert_                    (InstErrorCode.is_ok (success))
        self.assert_                    (agent_state == AgentState.OBSERVATORY_MODE)
        
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
        cmd                             = [AgentCommand.TRANSITION, AgentEvent.RESET]
        reply                           = yield self.ia_client.execute_observatory (cmd, tid)
        success                         = reply['success']
        result                          = reply['result']
        self.assert_                    (InstErrorCode.is_ok (success))

        # Check agent state.
        params                          = [AgentStatus.AGENT_STATE]
        reply                           = yield self.ia_client.get_observatory_status (params, tid)
        success                         = reply['success']
        result                          = reply['result']
        agent_state                     = result[AgentStatus.AGENT_STATE][1]
        self.assert_                    (InstErrorCode.is_ok (success))
        self.assert_                    (agent_state == AgentState.UNINITIALIZED)

        # End the transaction.
        reply                           = yield self.ia_client.end_transaction (tid)
        success                         = reply['success']
        self.assert_                    (InstErrorCode.is_ok (success))
        
        
    @defer.inlineCallbacks
    def test_execute_instrument (self):
        """
        Test cases for exectuing device commands through the instrument agent.
        """
        log.info("\n||||||| TestNMEA0183Agent.test_execute_instrument\n")

        # Check agent state upon creation. No transaction needed for get operation.
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
        self.assertIsInstance(result.get('temperature',None),float)
        self.assertIsInstance(result.get('salinity',None),float)
        self.assertIsInstance(result.get('sound_velocity',None),float)
        self.assertIsInstance(result.get('pressure',None),float)
        self.assertIsInstance(result.get('conductivity',None),float)
        self.assertIsInstance(result.get('time',None),str)
        self.assertIsInstance(result.get('date',None),str)
        
        # Start autosampling.
        chans = [DriverChannel.INSTRUMENT]
        cmd = [DriverCommand.START_AUTO_SAMPLING]
        reply = yield self.ia_client.execute_device(chans,cmd,tid)
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))

        #print 'autosampling started'
        
        # Wait for a few samples to arrive.
        yield pu.asleep(30)
        
        # Stop autosampling.
        chans = [DriverChannel.INSTRUMENT]
        cmd = [DriverCommand.STOP_AUTO_SAMPLING,'GETDATA']
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
            self.assertIsInstance(sample.get('time',None),str)
            self.assertIsInstance(sample.get('date',None),str)
        
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

        log.debug("||||||| TestNMEA0183Agent.test_get_capabilities")

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
        
        self.assertEqual (list (result[InstrumentCapability.DEVICE_CHANNELS][1]).sort(), NMEADeviceChannel.list().sort())
        self.assertEqual (list (result[InstrumentCapability.DEVICE_COMMANDS][1]).sort(), NMEADeviceCommand.list().sort())
        self.assertEqual (list (result[InstrumentCapability.DEVICE_METADATA][1]).sort(), NMEADeviceMetadataParameter.list().sort())
        self.assertEqual (list (result[InstrumentCapability.DEVICE_PARAMS][1]).sort(),   NMEADeviceParam.list().sort())
        self.assertEqual (list (result[InstrumentCapability.DEVICE_STATUSES][1]).sort(), NMEADeviceStatus.list().sort())
        self.assertEqual (list (result[InstrumentCapability.OBSERVATORY_COMMANDS][1]).sort(), AgentCommand.list().sort())
        self.assertEqual (list (result[InstrumentCapability.OBSERVATORY_METADATA][1]).sort(), MetadataParameter.list().sort())
        self.assertEqual (list (result[InstrumentCapability.OBSERVATORY_PARAMS][1]).sort(), AgentParameter.list().sort())
        self.assertEqual (list (result[InstrumentCapability.OBSERVATORY_STATUSES][1]).sort(), AgentStatus.list().sort())

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

