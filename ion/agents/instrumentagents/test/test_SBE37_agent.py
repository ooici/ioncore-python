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
from ion.core.exception import ReceivedError
import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.agents.instrumentagents.instrument_constants import AgentCommand
from ion.agents.instrumentagents.instrument_constants import AgentEvent
from ion.agents.instrumentagents.instrument_constants import DriverChannel
from ion.agents.instrumentagents.instrument_constants import DriverCommand
from ion.agents.instrumentagents.instrument_constants import InstErrorCode

log = ion.util.ionlog.getLogger(__name__)

    
"""
List of mac addresses for machines which should run these tests. If no
mac address of a NIC on the machine running the tests matches one in this
list, the tests are skipped. This is to prevent the trial robot from
commanding the instrument hardware, forcing these tests to be run
intentionally. Add the mac address of your development machine as
returned by ifconfig to cause the tests to run for you locally.
"""

allowed_mac_addr_list = [
    '00:26:bb:19:83:33'         # Edward's Macbook
    ]

mac_addr_pattern = r'\b\w\w[:\-]\w\w[:\-]\w\w[:\-]\w\w[:\-]\w\w[:\-]\w\w\b'
mac_addr_re = re.compile(mac_addr_pattern,re.MULTILINE)
mac_addr_list = mac_addr_re.findall(os.popen('ifconfig').read())
RUN_TESTS = any([addr in allowed_mac_addr_list for addr in mac_addr_list])


# It is useful to be able to easily turn tests on and off
# during development. Also this will ensure tests do not run
# automatically. 
SKIP_TESTS = [
    #'test_execute_instrument',
    'dummy'
]    

class TestSBE37Agent(IonTestCase):

    # Increase the timeout so we can handle longer instrument interactions.
    timeout = 180


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
        

    @defer.inlineCallbacks
    def tearDown(self):
        
        pu.asleep(1)
        yield self._stop_container()
        
        
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

        # Begin an explicit transaction.
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)

        # Issue state transition commands to bring the agent into
        # observatory mode.
        
        # Initialize the agent.
        cmd = [AgentCommand.TRANSITION,AgentEvent.INITIALIZE]
        reply = yield self.ia_client.execute_observatory(cmd,transaction_id) 
        success = reply['success']
        result = reply['result']
        
        #print 'init reply:'
        #print reply
        
        self.assert_(InstErrorCode.is_ok(success))
        
        # Connect to the device.
        cmd = [AgentCommand.TRANSITION,AgentEvent.GO_ACTIVE]
        reply = yield self.ia_client.execute_observatory(cmd,transaction_id) 
        success = reply['success']
        result = reply['result']

        #print 'go active reply:'
        #print reply

        self.assert_(InstErrorCode.is_ok(success))
        
        # Clear the driver state.
        cmd = [AgentCommand.TRANSITION,AgentEvent.CLEAR]
        reply = yield self.ia_client.execute_observatory(cmd,transaction_id) 
        success = reply['success']
        result = reply['result']

        #print 'clear reply:'
        #print reply

        self.assert_(InstErrorCode.is_ok(success))

        # Start observatory mode.
        cmd = [AgentCommand.TRANSITION,AgentEvent.RUN]
        reply = yield self.ia_client.execute_observatory(cmd,transaction_id) 
        success = reply['success']
        result = reply['result']

        #print 'run reply:'
        #print reply

        self.assert_(InstErrorCode.is_ok(success))
        
        # Get driver parameters.
        params = [('all','all')]
        reply = yield self.ia_client.get_device(params,transaction_id)
        success = reply['success']
        result = reply['result']

        # Strip off individual success vals to create a set params to
        # restore original config later.
        orig_config = dict(map(lambda x : (x[0],x[1][1]),result.items()))

        #print 'get device reply:'
        #print reply
        #print orig_config

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
        
        reply = yield self.ia_client.set_device(params,transaction_id)
        success = reply['success']
        result = reply['result']
        setparams = params
        
        #print 'set device reply:'
        #print reply

        self.assert_(InstErrorCode.is_ok(success))

        # Verify the set changes were made.
        params = [('all','all')]
        reply = yield self.ia_client.get_device(params,transaction_id)
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
        reply = yield self.ia_client.execute_device(chans,cmd,transaction_id)
        success = reply['success']
        result = reply['result']        

        #print 'acquisition result'
        #print result

        self.assert_(InstErrorCode.is_ok(success))
        self.assertIsInstance(result.get('temperature',None),float)
        self.assertIsInstance(result.get('salinity',None),float)
        self.assertIsInstance(result.get('sound velocity',None),float)
        self.assertIsInstance(result.get('pressure',None),float)
        self.assertIsInstance(result.get('conductivity',None),float)
        self.assertIsInstance(result.get('time',None),tuple)
        self.assertIsInstance(result.get('date',None),tuple)
        
        # Start autosampling.
        chans = [DriverChannel.INSTRUMENT]
        cmd = [DriverCommand.START_AUTO_SAMPLING]
        reply = yield self.ia_client.execute_device(chans,cmd,transaction_id)
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
            reply = yield self.ia_client.execute_device(chans,cmd,
                                                        transaction_id)
            success = reply['success']
            result = reply['result']
            
            if InstErrorCode.is_ok(success):
                break
            
            elif success == InstErrorCode.TIMEOUT:
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
            self.assertIsInstance(sample.get('sound velocity',None),float)
            self.assertIsInstance(sample.get('conductivity',None),float)
            self.assertIsInstance(sample.get('time',None),tuple)
            self.assertIsInstance(sample.get('date',None),tuple)
        
        # Restore original configuration.
        reply = yield self.ia_client.set_device(orig_config,transaction_id)
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))

        # Verify the original configuration was restored.
        
        params = [('all','all')]
        reply = yield self.ia_client.get_device(params,transaction_id)
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

        #print 'original configuration restored'
                
        # Disconnect from device.
        cmd = [AgentCommand.TRANSITION,AgentEvent.GO_INACTIVE]
        reply = yield self.ia_client.execute_observatory(cmd,transaction_id) 
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        #print 'go inactive reply:'
        #print reply
                
        # Close the transaction.
        reply = yield self.ia_client.end_transaction(transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))

