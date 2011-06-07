#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/test/test_SBE37.py
@brief Test cases for the SBE37 driver.
@author Edward Hunter
"""

import re
import os

from twisted.internet import defer
from twisted.trial import unittest

import ion.util.ionlog
import ion.util.procutils as pu
from ion.test.iontest import IonTestCase
from ion.agents.instrumentagents.SBE37_driver import SBE37DriverClient
from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator
from ion.agents.instrumentagents.SBE37_driver import DriverException
from ion.agents.instrumentagents.SBE37_driver import SBE37State
from ion.agents.instrumentagents.SBE37_driver import SBE37Channel
from ion.agents.instrumentagents.SBE37_driver import SBE37Command
from ion.agents.instrumentagents.SBE37_driver import SBE37Status
from ion.agents.instrumentagents.SBE37_driver import SBE37Capability
from ion.agents.instrumentagents.SBE37_driver import SBE37Parameter
from ion.agents.instrumentagents.SBE37_driver import SBE37MetadataParameter
from ion.agents.instrumentagents.instrument_constants import InstErrorCode
from ion.agents.instrumentagents.instrument_constants import ObservatoryState

log = ion.util.ionlog.getLogger(__name__)


def dump_dict(d,d2=None):
    print
    for (key,val) in d.iteritems():
        if d2:
            print key, ' ', val, ' ',d2.get(key,None)            
        else:
            print key, ' ', val


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
    'test_configure',
    'test_connect',
    'test_get_set',
    'test_get_metadata',
    'test_get_status',
    'test_get_capabilities',
    #'test_execute',
    'test_execute_direct',
    'dummy'
]


class TestSBE37(IonTestCase):
    
    # Increase the timeout so we can handle longer instrument interactions.
    timeout = 120
    
    # The instrument ID.
    instrument_id = '123'
    
    # Instrument and simulator configuration.
    
    simulator_port = 9000
    simulator_host = 'localhost'
    
    sbe_host = '137.110.112.119'
    sbe_port = 4001
    
    simulator_config = {
        'ipport':simulator_port, 
        'ipaddr':simulator_host
    }
    
    sbe_config = {
        'ipport':sbe_port, 
        'ipaddr':sbe_host
    }

    bogus_config = {
        'ipport':-99,
        'ipaddr':-100
        
    }
    
    @defer.inlineCallbacks
    def setUp(self):
                

        yield self._start_container()
        
        self.simulator = Simulator(self.instrument_id,self.simulator_port)        
        simulator_ports = self.simulator.start()
        simulator_port = simulator_ports[0]
        self.assertNotEqual(simulator_port,0)
        if simulator_port != self.simulator_port:
            self.simulator_port = simulator_port
            
        
        services = [
            {'name':'SBE37_driver',
             'module':'ion.agents.instrumentagents.SBE37_driver',
             'class':'SBE37Driver',
             'spawnargs':{}}
            ]


        self.sup = yield self._spawn_processes(services)

        self.driver_pid = yield self.sup.get_child_id('SBE37_driver')

        self.driver_client = SBE37DriverClient(proc=self.sup,
                                               target=self.driver_pid)



    @defer.inlineCallbacks
    def tearDown(self):
        
        
        yield self.simulator.stop()        
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_configure(self):
        """
        Test driver configure functions.
        """
        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_configure' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)
        
        # Configure the driver and verify.
        reply = yield self.driver_client.configure(self.sbe_config)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
                
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,self.sbe_config)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)
        
        # Try to set the configuration to a bogus value. This should fail.
        reply = yield self.driver_client.configure(self.bogus_config)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_error(success))
        self.assertEqual(result,self.bogus_config)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)

        # Set the configuration to a valid value in the disconnected state.
        reply = yield self.driver_client.configure(self.sbe_config)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
                
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,self.sbe_config)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)
        
    
    @defer.inlineCallbacks
    def test_connect(self):
        """
        Test driver connect to device.
        """
        
        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_connect' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(self.sbe_config)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,self.sbe_config)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)

        
        # Establish connection to device and verify.
        try:
            reply = yield self.driver_client.connect()
            
        except Exception, ex:
            self.fail('Could not connect to the device.')
            
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)
        
        
    @defer.inlineCallbacks
    def test_get_set(self):
        """
        Test driver get/set functions. 
        """

        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_get_set' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')


        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(self.sbe_config)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        print success
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,self.sbe_config)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)

        # Establish connection to device. This starts a loop of
        # wakeups to the device handled when the prompt returns.
        try:
            reply = yield self.driver_client.connect()
        except:
            self.fail('Could not connect to the device.')
                
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.CONNECTED)

        timeout = 60
        
        # Get all parameters and verify. Store the current config for later.
        params = [(SBE37Channel.ALL,SBE37Parameter.ALL)]

        reply = yield self.driver_client.get(params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        config = result
        config = dict(map(lambda x: (x[0],x[1][1]),config.items()))
                
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(all(map(lambda x: x[1] != None,result.values())),True)
        self.assertEqual(current_state,SBE37State.CONNECTED)

        # Get all pressure parameters and verify.        
        params = [(SBE37Channel.PRESSURE,SBE37Parameter.ALL)]
        reply = yield self.driver_client.get(params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        pressure_params = [
            (SBE37Channel.PRESSURE,'PCALDATE'),
            (SBE37Channel.PRESSURE,'PA0'),
            (SBE37Channel.PRESSURE,'PA1'),
            (SBE37Channel.PRESSURE,'PA2'),
            (SBE37Channel.PRESSURE,'PTCA0'),
            (SBE37Channel.PRESSURE,'PTCA1'),
            (SBE37Channel.PRESSURE,'PTCA2'),
            (SBE37Channel.PRESSURE,'PTCB0'),
            (SBE37Channel.PRESSURE,'PTCB1'),
            (SBE37Channel.PRESSURE,'PTCB2'),
            (SBE37Channel.PRESSURE,'POFFSET')            
            ]
                
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(pressure_params.sort(),result.keys().sort())
        self.assertEqual(all(map(lambda x: x[1] != None ,result.values())),True)
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Get a few parameters by name and verify.
        params = [
            (SBE37Channel.INSTRUMENT,'NAVG'),
            (SBE37Channel.INSTRUMENT,'INTERVAL'),
            (SBE37Channel.INSTRUMENT,'OUTPUTSV'),
            (SBE37Channel.TEMPERATURE,'TA0'),
            (SBE37Channel.CONDUCTIVITY,'WBOTC')
            ]

        reply = yield self.driver_client.get(params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(params.sort(),result.keys().sort())
        self.assertEqual(all(map(lambda x: x[1] != None ,result.values())),True)
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Set a few parameters and verify.
        orig_params = {
            (SBE37Channel.INSTRUMENT,'NAVG'):\
                config[(SBE37Channel.INSTRUMENT,'NAVG')],
            (SBE37Channel.INSTRUMENT,'INTERVAL'):\
                config[(SBE37Channel.INSTRUMENT,'INTERVAL')],
            (SBE37Channel.INSTRUMENT,'OUTPUTSV'):\
                config[(SBE37Channel.INSTRUMENT,'OUTPUTSV')],
            (SBE37Channel.TEMPERATURE,'TA0'):\
                config[(SBE37Channel.TEMPERATURE,'TA0')],
            (SBE37Channel.CONDUCTIVITY,'WBOTC'):\
                config[(SBE37Channel.CONDUCTIVITY,'WBOTC')]            
        }
        new_params = {}
        new_params[(SBE37Channel.INSTRUMENT,'NAVG')] = \
            orig_params[(SBE37Channel.INSTRUMENT,'NAVG')] + 1
        new_params[(SBE37Channel.INSTRUMENT,'INTERVAL')] = \
            orig_params[(SBE37Channel.INSTRUMENT,'INTERVAL')] + 1
        new_params[(SBE37Channel.INSTRUMENT,'OUTPUTSV')] = \
            not orig_params[(SBE37Channel.INSTRUMENT,'OUTPUTSV')]
        new_params[(SBE37Channel.TEMPERATURE,'TA0')] = \
            2*float(orig_params[(SBE37Channel.TEMPERATURE,'TA0')])
        new_params[(SBE37Channel.CONDUCTIVITY,'WBOTC')] = \
            2*float(orig_params[(SBE37Channel.CONDUCTIVITY,'WBOTC')])
                
        reply = yield self.driver_client.set(new_params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(new_params.keys().sort(),result.keys().sort())
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Get all parameters, verify the changes were made.        
        params = [(SBE37Channel.ALL,SBE37Parameter.ALL)]
        reply = yield self.driver_client.get(params,timeout)
        get_current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertAlmostEqual(result[(SBE37Channel.CONDUCTIVITY,'WBOTC')][1],
            new_params[(SBE37Channel.CONDUCTIVITY,'WBOTC')],4)
        self.assertAlmostEqual(result[(SBE37Channel.TEMPERATURE,'TA0')][1],
            new_params[(SBE37Channel.TEMPERATURE,'TA0')],4)
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'NAVG')][1],
            new_params[(SBE37Channel.INSTRUMENT,'NAVG')])
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'INTERVAL')][1],
            new_params[(SBE37Channel.INSTRUMENT,'INTERVAL')])
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'OUTPUTSV')][1],
            new_params[(SBE37Channel.INSTRUMENT,'OUTPUTSV')])
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Restore original state.         
        reply = yield self.driver_client.set(orig_params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(orig_params.keys().sort(),result.keys().sort())
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Get parameters and make sure they match the original config.
        params = [(SBE37Channel.ALL,SBE37Parameter.ALL)]
        reply = yield self.driver_client.get(params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertAlmostEqual(result[(SBE37Channel.CONDUCTIVITY,'WBOTC')][1],
            config[(SBE37Channel.CONDUCTIVITY,'WBOTC')],4)
        self.assertAlmostEqual(result[(SBE37Channel.TEMPERATURE,'TA0')][1],
            config[(SBE37Channel.TEMPERATURE,'TA0')],4)
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'NAVG')][1],
            config[(SBE37Channel.INSTRUMENT,'NAVG')])
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'INTERVAL')][1],
            config[(SBE37Channel.INSTRUMENT,'INTERVAL')])
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'OUTPUTSV')][1],
            config[(SBE37Channel.INSTRUMENT,'OUTPUTSV')])
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Try getting bad parameters.This should fail for bad parameters but
        # succeed for the good ones.
        params = [
            ('I am a bad channel name','NAVG'),
            (SBE37Channel.INSTRUMENT,'INTERVAL'),
            (SBE37Channel.INSTRUMENT,'I am a bad parameter name'),
            (SBE37Channel.TEMPERATURE,'TA0'),
            (SBE37Channel.CONDUCTIVITY,'WBOTC')
            ]
        reply = yield self.driver_client.get(params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_error(success))
        self.assert_(InstErrorCode.is_error(result[('I am a bad channel name',
            'NAVG')][0]))
        self.assertEqual(result[('I am a bad channel name','NAVG')][1],None)
        self.assert_(InstErrorCode.is_ok(result[(SBE37Channel.INSTRUMENT,
            'INTERVAL')][0]))
        self.assertIsInstance(result[(SBE37Channel.INSTRUMENT,'INTERVAL')][1],int)
        self.assert_(InstErrorCode.is_error(result[(SBE37Channel.INSTRUMENT,
            'I am a bad parameter name')][0]))
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,
            'I am a bad parameter name')][1],None)
        self.assert_(InstErrorCode.is_ok(result[(SBE37Channel.TEMPERATURE,
            'TA0')][0]))
        self.assertIsInstance(result[(SBE37Channel.TEMPERATURE,'TA0')][1],float)
        self.assert_(InstErrorCode.is_ok(result[(SBE37Channel.CONDUCTIVITY,
            'WBOTC')][0]))
        self.assertIsInstance(result[(SBE37Channel.CONDUCTIVITY,'WBOTC')][1],
            float)
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Try setting bad parameters. This should fail for bad parameters
        # but succeed for valid ones.
        bad_params = new_params
        bad_params[('I am a bad channel','NAVG')] = \
            bad_params[(SBE37Channel.INSTRUMENT,'NAVG')]
        del bad_params[(SBE37Channel.INSTRUMENT,'NAVG')]
        bad_params[(SBE37Channel.TEMPERATURE,'I am a bad parameter')] = \
            bad_params[(SBE37Channel.TEMPERATURE,'TA0')]
        del bad_params[(SBE37Channel.TEMPERATURE,'TA0')]
        
        reply = yield self.driver_client.set(bad_params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_error(success))
        self.assert_(InstErrorCode.is_error(result[('I am a bad channel',
                                                    'NAVG')]))
        self.assert_(InstErrorCode.is_ok(result[(SBE37Channel.INSTRUMENT,
                                                 'INTERVAL')]))
        self.assert_(InstErrorCode.is_ok(result[(SBE37Channel.INSTRUMENT,
                                                 'OUTPUTSV')]))
        self.assert_(InstErrorCode.is_error(result[(SBE37Channel.TEMPERATURE,
                                                    'I am a bad parameter')]))
        self.assert_(InstErrorCode.is_ok(result[(SBE37Channel.CONDUCTIVITY,
                                                 'WBOTC')]))

        # Get all parameters, verify the valid ones were set,
        # and the invalid ones kept the old values.
        params = [(SBE37Channel.ALL,SBE37Parameter.ALL)]
        reply = yield self.driver_client.get(params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'INTERVAL')][1],
                         new_params[(SBE37Channel.INSTRUMENT,'INTERVAL')])
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'OUTPUTSV')][1],
                         new_params[(SBE37Channel.INSTRUMENT,'OUTPUTSV')])
        self.assertAlmostEqual(result[(SBE37Channel.CONDUCTIVITY,'WBOTC')][1],
                               new_params[(SBE37Channel.CONDUCTIVITY,'WBOTC')],4)
        self.assertEqual(result[(SBE37Channel.INSTRUMENT,'NAVG')][1],
                         orig_params[(SBE37Channel.INSTRUMENT,'NAVG')])
        self.assertAlmostEqual(result[(SBE37Channel.TEMPERATURE,'TA0')][1],
                               orig_params[(SBE37Channel.TEMPERATURE,'TA0')],4)
        self.assertEqual(current_state,SBE37State.CONNECTED)

        # Restore the original configuration and verify.
        # This should set all the parameters in the driver.
        # In addition to restoring original, it tests that each parameter
        # can be set.
        
        reply = yield self.driver_client.set(config,timeout) 
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(config.keys().sort(),result.keys().sort())
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)


    @defer.inlineCallbacks
    def test_get_status(self):
        """
        Test driver op_get_status.
        """

        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_get_status' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(self.sbe_config)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,self.sbe_config)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)


        # Establish connection to device and verify.
        try:
            reply = yield self.driver_client.connect()
        except:
            self.fail('Could not connect to the device.')
            
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.CONNECTED)

        # Test get status. Currently not implemented.
        params = [(SBE37Channel.ALL,SBE37Status.ALL)]
        reply = yield self.driver_client.get_status(params)
        success = reply['success']
        result = reply['result']
        print reply
        self.assert_(InstErrorCode.is_ok(success))
        
        dvr_state = result[(SBE37Channel.INSTRUMENT,
                            SBE37Status.DRIVER_STATE)][1]
        dvr_version = result[(SBE37Channel.INSTRUMENT,
                              SBE37Status.DRIVER_VERSION)][1]
        obs_state = result[(SBE37Channel.INSTRUMENT,
                            SBE37Status.OBSERVATORY_STATE)][1]
        dvr_alarms = result[(SBE37Channel.INSTRUMENT,
                             SBE37Status.DRIVER_ALARMS)][1]
        
        self.assert_(SBE37State.has(dvr_state))
        self.assertIsInstance(dvr_version,str)
        self.assert_(ObservatoryState.has(obs_state))
        self.assertIsInstance(dvr_alarms,(list,tuple))
        
        # Try to get some bad status vals. This should fail for those that
        # are bad and work for the good ones.
        params = [
            (SBE37Channel.INSTRUMENT,SBE37Status.DRIVER_STATE),
            (SBE37Channel.INSTRUMENT,SBE37Status.DRIVER_VERSION),
            (SBE37Channel.INSTRUMENT,'Bad status name'),
            ('Bad channel name',SBE37Status.DRIVER_ALARMS),
            (SBE37Channel.CONDUCTIVITY,SBE37Status.OBSERVATORY_STATE),
        ]
        reply = yield self.driver_client.get_status(params)
        success = reply['success']
        result = reply['result']        
        self.assert_(InstErrorCode.is_error(success))


                
        dvr_state = result[(SBE37Channel.INSTRUMENT,
                            SBE37Status.DRIVER_STATE)]
        dvr_version = result[(SBE37Channel.INSTRUMENT,
                              SBE37Status.DRIVER_VERSION)]
        bad_status = result[(SBE37Channel.INSTRUMENT,
                            'Bad status name')]
        bad_chan = result[('Bad channel name',
                             SBE37Status.DRIVER_ALARMS)]
        mixed_up = result[(SBE37Channel.CONDUCTIVITY,
                           SBE37Status.OBSERVATORY_STATE)]
        self.assert_(InstErrorCode.is_ok(dvr_state[0]))
        self.assert_(SBE37State.has(dvr_state[1]))
        self.assert_(InstErrorCode.is_ok(dvr_version[0]))
        self.assertIsInstance(dvr_version[1],str)
        self.assert_(InstErrorCode.is_error(bad_status[0]))
        self.assertEqual(bad_status[1],None)
        self.assert_(InstErrorCode.is_error(bad_chan[0]))
        self.assertEqual(bad_chan[1],None)
        self.assert_(InstErrorCode.is_error(mixed_up[0]))
        self.assertEqual(mixed_up[1],None)
        
        
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)


    @defer.inlineCallbacks
    def test_get_capabilities(self):
        """
        Test driver op_get_capabilities.
        """

        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_get_capabilities' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        params = self.sbe_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,params)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)


        # Establish connection to device and verify.
        try:
            reply = yield self.driver_client.connect()
        except:
            self.fail('Could not connect to the device.')
            
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.CONNECTED)

        # Test get capabilities. Currently not implemented.
        params = [SBE37Capability.DEVICE_ALL]
        reply = yield self.driver_client.get_capabilities(params)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result.keys().sort(),SBE37Capability.list().sort())
        self.assertEqual(list(result[SBE37Capability.DEVICE_COMMANDS]).sort(),
                         SBE37Command.list().sort())
        self.assertEqual(list(result[SBE37Capability.DEVICE_METADATA]).sort(),
                         SBE37MetadataParameter.list().sort())
        self.assertEqual(list(result[SBE37Capability.DEVICE_PARAMS]).sort(),
                         SBE37Parameter.list().sort())
        self.assertEqual(list(result[SBE37Capability.DEVICE_STATUSES]).sort(),
                         SBE37Status.list().sort())
        self.assertEqual(list(result[SBE37Capability.DEVICE_CHANNELS]).sort(),
                         SBE37Channel.list().sort())
        
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)

    
    @defer.inlineCallbacks
    def test_get_metadata(self):
        """
        Test driver op_get_metadata.
        """

        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_get_metadata' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        params = self.sbe_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,params)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)


        # Establish connection to device and verify.
        try:
            reply = yield self.driver_client.connect()
        except:
            self.fail('Could not connect to the device.')
            
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.CONNECTED)

        # Test get metadata. Currently not implemented.
        params = [(SBE37Channel.ALL,SBE37Parameter.ALL,
                   SBE37MetadataParameter.ALL)]
        reply = yield self.driver_client.get_metadata(params)
        success = reply['success']
        result = reply['result']        
        self.assert_(InstErrorCode.is_equal(success,
                                            InstErrorCode.NOT_IMPLEMENTED))
        
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)


    @defer.inlineCallbacks
    def test_execute(self):
        """
        Test driver execute functions.
        """

        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_execute' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        timeout = 60

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)
        
        # Configure the driver and verify.
        reply = yield self.driver_client.configure(self.sbe_config)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,self.sbe_config)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)

        # Establish connection to device and verify.
        try:
            reply = yield self.driver_client.connect()
        except:
            self.fail('Could not connect to the device.')
            
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Make sure the parameters are in a reasonable state for sampling.
        # Set a few parameters and verify.
        params = {}
        params[(SBE37Channel.INSTRUMENT,'NAVG')] = 1
        params[(SBE37Channel.INSTRUMENT,'INTERVAL')] = 5
        params[(SBE37Channel.INSTRUMENT,'OUTPUTSV')] = True
        params[(SBE37Channel.INSTRUMENT,'OUTPUTSAL')] = True
        params[(SBE37Channel.INSTRUMENT,'TXREALTIME')] = True
        params[(SBE37Channel.INSTRUMENT,'STORETIME')] = True
        
        reply = yield self.driver_client.set(params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(current_state,SBE37State.CONNECTED)        

        # Get all the parameters and dump them to the screen if desired.
        params = [(SBE37Channel.ALL,SBE37Parameter.ALL)]
        reply = yield self.driver_client.get(params,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(current_state,SBE37State.CONNECTED)        

        # Comment this out for automated testing.
        # It is useful to examine the instrument parameters during
        # interactive testing.
        # dump_dict(result)
        
        # Acquire a polled sample and verify result.
        channels = [SBE37Channel.INSTRUMENT]
        command = [SBE37Command.ACQUIRE_SAMPLE]
        reply = yield self.driver_client.execute(channels,command,timeout)

        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertIsInstance(result,(list,tuple))
        self.assert_(len(result)==1)
        result = result[0]
        self.assertIsInstance(result.get('temperature',None),float)
        self.assertIsInstance(result.get('salinity',None),float)
        self.assertIsInstance(result.get('pressure',None),float)
        self.assertIsInstance(result.get('sound_velocity',None),float)
        self.assertIsInstance(result.get('conductivity',None),float)
        self.assertIsInstance(result.get('device_time',None),str)
        self.assertIsInstance(result.get('driver_time',None),str)
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Test and verify autosample mode.
        channels = [SBE37Channel.INSTRUMENT]
        command = [SBE37Command.START_AUTO_SAMPLING]
        reply = yield self.driver_client.execute(channels,command,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))

        # Wait for a few samples to arrive.
        yield pu.asleep(30)

        # Test and verify autosample exit and check sample data.
        channels = [SBE37Channel.INSTRUMENT]
        command = [SBE37Command.STOP_AUTO_SAMPLING,'GETDATA']
        
        reply = yield self.driver_client.execute(channels,command,timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
                                
        # We succeeded in completing the stop. Verify the samples recovered.
        self.assert_(InstErrorCode.is_ok(success))
        for sample in result:
            self.assertIsInstance(sample.get('temperature'),float)
            self.assertIsInstance(sample.get('salinity'),float)
            self.assertIsInstance(sample.get('pressure',None),float)
            self.assertIsInstance(sample.get('sound_velocity',None),float)
            self.assertIsInstance(sample.get('conductivity',None),float)
            self.assertIsInstance(sample.get('device_time',None),str)
            self.assertIsInstance(sample.get('driver_time',None),str)
        self.assertEqual(current_state,SBE37State.CONNECTED)
                
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)
        
        
    @defer.inlineCallbacks
    def test_execute_direct(self):
        """
        Test direct executes function.
        """

        if not RUN_TESTS:
            raise unittest.SkipTest("Do not run this test automatically.")
        
        if 'test_execute_direct' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        params = self.sbe_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)
        
        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,params)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)

        # Establish connection to device and verify.
        try:
            reply = yield self.driver_client.connect()
        except:
            self.fail('Could not connect to the device.')
            
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.CONNECTED)
        
        # Send raw bytes commands to the device.
        bytes = 'to_be_done'
        timeout = 60
        reply = yield self.driver_client.execute_direct(bytes,timeout)
            
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_equal(success,
                                            InstErrorCode.NOT_IMPLEMENTED))

        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)

        
        
    """
    The following is for experimenting with error callbacks only and not
    intended as part of the unit test regime.
    @defer.inlineCallbacks
    def test_errors(self):

        #if not RUN_TESTS:
        #    raise unittest.SkipTest("Do not run this test automatically.")
        raise unittest.SkipTest("Do not run this test automatically.")


        params = self.sbe_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,SBE37State.UNCONFIGURED)

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assertEqual(success[0],'OK')
        self.assertEqual(result,params)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)


        # Establish connection to device and verify.
        try:
            reply = yield self.driver_client.connect()
        except:
            self.fail('Could not connect to the device.')
            
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.CONNECTED)


        params = {
            'command':['DRIVER_CMD_TEST_ERRORS'],
            'channels':[SBE37Channel.INSTRUMENT]
                }
        
        try:
            reply = yield self.driver_client.execute(params)
        except Exception, e:
            print '***exception'
            print e
            print '***type'
            print type(e)
            print '***dir'
            print dir(e)
            print '***message'
            print e.message
            print '***content'
            print e.msg_content
        else:
            print reply

        
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(result,None)
        self.assertEqual(current_state,SBE37State.DISCONNECTED)
    """


