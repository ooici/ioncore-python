#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/test/test_SBE49.py
@brief This module has test cases to test out SeaBird SBE49 instrument software
    including the driver. This assumes that generic InstrumentAgent code has
    been tested by another test case
@author Steve Foley
@see ion.agents.instrumentagents.test.test_instrument
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.test.iontest import IonTestCase

from ion.agents.instrumentagents.SBE37_driver import SBE37DriverClient
from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator

from ion.services.dm.distribution.pubsub_service import PubSubClient

import ion.util.procutils as pu
from ion.resources.dm_resource_descriptions import PubSubTopicResource, SubscriptionResource

from twisted.trial import unittest
import socket

def dump_dict(d,d2=None):
    print
    for (key,val) in d.iteritems():
        if d2:
            print key, ' ', val, ' ',d2.get(key,None)            
        else:
            print key, ' ', val


class TestSBE37(IonTestCase):
    
    # Increase the timeout so we can handle longer instrument interactions.
    timeout = 60
    
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

    
    @defer.inlineCallbacks
    def setUp(self):
                

        yield self._start_container()
        
        #fqdn = socket.get

        self.simulator = Simulator(self.instrument_id,self.simulator_port)        
        simulator_ports = self.simulator.start()
        simulator_port = simulator_ports[0]
        self.assertNotEqual(simulator_port,0)
        if simulator_port != self.simulator_port:
            self.simulator_port = simulator_port
            
        
        services = [
            {'name':'pubsub_service',
             'module':'ion.services.dm.distribution.pubsub_service',
             'class':'DataPubsubService'},

            {'name':'SBE37_driver',
             'module':'ion.agents.instrumentagents.SBE37_driver',
             'class':'SBE37Driver',
             'spawnargs':{}}
            ]


        self.sup = yield self._spawn_processes(services)

        self.driver_pid = yield self.sup.get_child_id('SBE37_driver')

        self.driver_client = SBE37DriverClient(proc=self.sup,target=self.driver_pid)

    @defer.inlineCallbacks
    def tearDown(self):
        
        
        yield self.simulator.stop()        
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_configure(self):


        raise unittest.SkipTest("Do not run this test automatically.")


        params = self.sbe_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,'STATE_UNCONFIGURED')

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
                
        self.assertEqual(success[0],'OK')
        self.assertEqual(result,params)
        self.assertEqual(current_state,'STATE_DISCONNECTED')

        
    
    @defer.inlineCallbacks
    def test_connect(self):

        raise unittest.SkipTest("Do not run this test automatically.")

        params = self.sbe_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,'STATE_UNCONFIGURED')

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assertEqual(success[0],'OK')
        self.assertEqual(result,params)
        self.assertEqual(current_state,'STATE_DISCONNECTED')


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
        self.assertEqual(current_state,'STATE_CONNECTED')

        
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(result,None)
        self.assertEqual(current_state,'STATE_DISCONNECTED')
        
    @defer.inlineCallbacks
    def test_get_set(self):
        """
        Note that this test changes instrument parameters and can leave them in an
        altered state if the test fails.
        """

        raise unittest.SkipTest("Do not run this test automatically.")


        params = self.sbe_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,'STATE_UNCONFIGURED')

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        
        self.assertEqual(success[0],'OK')
        self.assertEqual(result,params)
        self.assertEqual(current_state,'STATE_DISCONNECTED')


        # Establish connection to device. This starts a loop of
        # wakeups to the device handled when the prompt returns.
        try:
            reply = yield self.driver_client.connect()
        except:
            self.fail('Could not connect to the device.')
        
        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(result,None)
        self.assertEqual(current_state,'STATE_CONNECTED')


        # Get all parameters and verify. Store the current config for later.
        params = [('all','all')]

        reply = yield self.driver_client.get(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        config = result
        config = dict(map(lambda x: (x[0],x[1][1]),config.items()))
      
        
        self.assertEqual(success[0],'OK')
        self.assertEqual(all(map(lambda x: x[1] != None,result.values())),True)
        self.assertEqual(current_state,'STATE_CONNECTED')

        
        # Get all pressure parameters and verify.        
        params = [('CHAN_PRESSURE','all')]

        reply = yield self.driver_client.get(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
                
        pressure_params = [
            ('CHAN_PRESSURE','PCALDATE'),
            ('CHAN_PRESSURE','PA0'),
            ('CHAN_PRESSURE','PA1'),
            ('CHAN_PRESSURE','PA2'),
            ('CHAN_PRESSURE','PTCA0'),
            ('CHAN_PRESSURE','PTCA1'),
            ('CHAN_PRESSURE','PTCA2'),
            ('CHAN_PRESSURE','PTCB0'),
            ('CHAN_PRESSURE','PTCB1'),
            ('CHAN_PRESSURE','PTCB2'),
            ('CHAN_PRESSURE','POFFSET')            
            ]
                
                
        self.assertEqual(success[0],'OK')
        self.assertEqual(pressure_params.sort(),result.keys().sort())
        self.assertEqual(all(map(lambda x: x[1] != None ,result.values())),True)
        self.assertEqual(current_state,'STATE_CONNECTED')
        
        
        # Get a few parameters by name and verify.
        params = [
            ('CHAN_INSTRUMENT','NAVG'),
            ('CHAN_INSTRUMENT','INTERVAL'),
            ('CHAN_INSTRUMENT','OUTPUTSV'),
            ('CHAN_TEMPERATURE','TA0'),
            ('CHAN_CONDUCTIVITY','WBOTC')
            ]

        reply = yield self.driver_client.get(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assertEqual(success[0],'OK')
        self.assertEqual(params.sort(),result.keys().sort())
        self.assertEqual(all(map(lambda x: x[1] != None ,result.values())),True)
        self.assertEqual(current_state,'STATE_CONNECTED')
                        
                
        # Set a few parameters and verify.
        orig_params = {
            ('CHAN_INSTRUMENT','NAVG'):config[('CHAN_INSTRUMENT','NAVG')],
            ('CHAN_INSTRUMENT','INTERVAL'):config[('CHAN_INSTRUMENT','INTERVAL')],
            ('CHAN_INSTRUMENT','OUTPUTSV'):config[('CHAN_INSTRUMENT','OUTPUTSV')],
            ('CHAN_TEMPERATURE','TA0'):config[('CHAN_TEMPERATURE','TA0')],
            ('CHAN_CONDUCTIVITY','WBOTC'):config[('CHAN_CONDUCTIVITY','WBOTC')]            
        }
        new_params = {}
        new_params[('CHAN_INSTRUMENT','NAVG')] = orig_params[('CHAN_INSTRUMENT','NAVG')] + 1
        new_params[('CHAN_INSTRUMENT','INTERVAL')] = orig_params[('CHAN_INSTRUMENT','INTERVAL')] + 1
        new_params[('CHAN_INSTRUMENT','OUTPUTSV')] = not orig_params[('CHAN_INSTRUMENT','OUTPUTSV')]
        new_params[('CHAN_TEMPERATURE','TA0')] = 2*float(orig_params[('CHAN_TEMPERATURE','TA0')])
        new_params[('CHAN_CONDUCTIVITY','WBOTC')] = 2*float(orig_params[('CHAN_CONDUCTIVITY','WBOTC')])
        
        
        reply = yield self.driver_client.set(new_params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(new_params.keys().sort(),result.keys().sort())
        self.assertEqual(current_state,'STATE_CONNECTED')
        
        # Get all parameters, verify the changes were made.        
        params = [('all','all')]

        reply = yield self.driver_client.get(params)
        get_current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertAlmostEqual(result[('CHAN_CONDUCTIVITY','WBOTC')][1],new_params[('CHAN_CONDUCTIVITY','WBOTC')],4)
        self.assertAlmostEqual(result[('CHAN_TEMPERATURE','TA0')][1],new_params[('CHAN_TEMPERATURE','TA0')],4)
        self.assertEqual(result[('CHAN_INSTRUMENT','NAVG')][1],new_params[('CHAN_INSTRUMENT','NAVG')])
        self.assertEqual(result[('CHAN_INSTRUMENT','INTERVAL')][1],new_params[('CHAN_INSTRUMENT','INTERVAL')])
        self.assertEqual(result[('CHAN_INSTRUMENT','OUTPUTSV')][1],new_params[('CHAN_INSTRUMENT','OUTPUTSV')])
        self.assertEqual(current_state,'STATE_CONNECTED')
        
        # Restore original state.         
        reply = yield self.driver_client.set(orig_params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(orig_params.keys().sort(),result.keys().sort())
        self.assertEqual(current_state,'STATE_CONNECTED')
        
        # Get parameters and make sure they match the original config.
        params = [('all','all')]

        reply = yield self.driver_client.get(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assertEqual(success[0],'OK')
        self.assertAlmostEqual(result[('CHAN_CONDUCTIVITY','WBOTC')][1],config[('CHAN_CONDUCTIVITY','WBOTC')],4)
        self.assertAlmostEqual(result[('CHAN_TEMPERATURE','TA0')][1],config[('CHAN_TEMPERATURE','TA0')],4)
        self.assertEqual(result[('CHAN_INSTRUMENT','NAVG')][1],config[('CHAN_INSTRUMENT','NAVG')])
        self.assertEqual(result[('CHAN_INSTRUMENT','INTERVAL')][1],config[('CHAN_INSTRUMENT','INTERVAL')])
        self.assertEqual(result[('CHAN_INSTRUMENT','OUTPUTSV')][1],config[('CHAN_INSTRUMENT','OUTPUTSV')])
        self.assertEqual(current_state,'STATE_CONNECTED')
        
                
        # Try getting bad parameters.This should fail for bad parameters but
        # succeed for the good ones.
        params = [
            ('I am a bad channel name','NAVG'),
            ('CHAN_INSTRUMENT','INTERVAL'),
            ('CHAN_INSTRUMENT','I am a bad parameter name'),
            ('CHAN_TEMPERATURE','TA0'),
            ('CHAN_CONDUCTIVITY','WBOTC')
            ]
        reply = yield self.driver_client.get(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assertEqual(success[0],'ERROR')
        self.assertEqual(result[('I am a bad channel name','NAVG')][0][0],'ERROR')
        self.assertEqual(result[('I am a bad channel name','NAVG')][1],None)
        self.assertEqual(result[('CHAN_INSTRUMENT','INTERVAL')][0][0],'OK')
        self.assertIsInstance(result[('CHAN_INSTRUMENT','INTERVAL')][1],int)
        self.assertEqual(result[('CHAN_INSTRUMENT','I am a bad parameter name')][0][0],'ERROR')
        self.assertEqual(result[('CHAN_INSTRUMENT','I am a bad parameter name')][1],None)
        self.assertEqual(result[('CHAN_TEMPERATURE','TA0')][0][0],'OK')
        self.assertIsInstance(result[('CHAN_TEMPERATURE','TA0')][1],float)
        self.assertEqual(result[('CHAN_CONDUCTIVITY','WBOTC')][0][0],'OK')
        self.assertIsInstance(result[('CHAN_CONDUCTIVITY','WBOTC')][1],float)
        self.assertEqual(current_state,'STATE_CONNECTED')
        
        
        # Try setting bad parameters. This should fail for bad parameters but succeed for
        # valid ones.
        bad_params = new_params
        bad_params[('I am a bad channel','NAVG')] = bad_params[('CHAN_INSTRUMENT','NAVG')]
        del bad_params[('CHAN_INSTRUMENT','NAVG')]
        bad_params[('CHAN_TEMPERATURE','I am a bad parameter')] = bad_params[('CHAN_TEMPERATURE','TA0')]
        del bad_params[('CHAN_TEMPERATURE','TA0')]
        
        reply = yield self.driver_client.set(bad_params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'ERROR')
        self.assertEqual(result[('I am a bad channel','NAVG')][0],'ERROR')
        self.assertEqual(result[('CHAN_INSTRUMENT','INTERVAL')][0],'OK')
        self.assertEqual(result[('CHAN_INSTRUMENT','OUTPUTSV')][0],'OK')
        self.assertEqual(result[('CHAN_TEMPERATURE','I am a bad parameter')][0],'ERROR')
        self.assertEqual(result[('CHAN_CONDUCTIVITY','WBOTC')][0],'OK')

        # Get all parameters, verify the valid ones were set, and the invalid ones kept the
        # old values.
        params = [('all','all')]
        reply = yield self.driver_client.get(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(result[('CHAN_INSTRUMENT','INTERVAL')][1],new_params[('CHAN_INSTRUMENT','INTERVAL')])
        self.assertEqual(result[('CHAN_INSTRUMENT','OUTPUTSV')][1],new_params[('CHAN_INSTRUMENT','OUTPUTSV')])
        self.assertAlmostEqual(result[('CHAN_CONDUCTIVITY','WBOTC')][1],new_params[('CHAN_CONDUCTIVITY','WBOTC')],4)
        self.assertEqual(result[('CHAN_INSTRUMENT','NAVG')][1],orig_params[('CHAN_INSTRUMENT','NAVG')])
        self.assertAlmostEqual(result[('CHAN_TEMPERATURE','TA0')][1],orig_params[('CHAN_TEMPERATURE','TA0')],4)
        self.assertEqual(current_state,'STATE_CONNECTED')

        # Restore the original configuration and verify.
        # This should set all the parameters in the driver.
        # In addition to restoring original, it tests that each parameter can be set.
        
        
        reply = yield self.driver_client.set(config) 
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assertEqual(success[0],'OK')
        self.assertEqual(config.keys().sort(),result.keys().sort())
        self.assertEqual(current_state,'STATE_CONNECTED')
        
        
        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(result,None)
        self.assertEqual(current_state,'STATE_DISCONNECTED')
        

    
    @defer.inlineCallbacks
    def test_execute(self):


        raise unittest.SkipTest("Do not run this test automatically.")

        params = self.sbe_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state,'STATE_UNCONFIGURED')

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)        
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assertEqual(success[0],'OK')
        self.assertEqual(result,params)
        self.assertEqual(current_state,'STATE_DISCONNECTED')


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
        self.assertEqual(current_state,'STATE_CONNECTED')

        # Make sure the parameters are in a reasonable state for sampling.
        # Set a few parameters and verify.
        params = {}
        params[('CHAN_INSTRUMENT','NAVG')] = 1
        params[('CHAN_INSTRUMENT','INTERVAL')] = 5
        params[('CHAN_INSTRUMENT','OUTPUTSV')] = True
        params[('CHAN_INSTRUMENT','OUTPUTSAL')] = True
        
        reply = yield self.driver_client.set(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(params.keys().sort(),result.keys().sort())
        self.assertEqual(current_state,'STATE_CONNECTED')        

        # Get all the parameters and dump them to the screen if desired.
        params = [('all','all')]
        reply = yield self.driver_client.get(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(current_state,'STATE_CONNECTED')        

        dump_dict(result)

        # Acquire a polled sample and verify result.
        params = {'channels':['CHAN_INSTRUMENT'],'command':['DRIVER_CMD_ACQUIRE_SAMPLE']}
        reply = yield self.driver_client.execute(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        
        self.assertEqual(success[0],'OK')
        self.assertIsInstance(result.get('temperature',None),float)
        self.assertIsInstance(result.get('salinity',None),float)
        self.assertIsInstance(result.get('sound velocity',None),float)
        self.assertIsInstance(result.get('pressure',None),float)
        self.assertIsInstance(result.get('conductivity',None),float)
        self.assertIsInstance(result.get('time',None),tuple)
        self.assertIsInstance(result.get('date',None),tuple)
        self.assertEqual(current_state,'STATE_CONNECTED')
        
        # Test and verify autosample mode.
        params = {'channels':['CHAN_INSTRUMENT'],'command':['DRIVER_CMD_START_AUTO_SAMPLING']}
        reply = yield self.driver_client.execute(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')

        # Wait for a few samples to arrive.
        yield pu.asleep(30)

        # Test and verify autosample exit and check sample data.
        params = {'channels':['CHAN_INSTRUMENT'],'command':['DRIVER_CMD_STOP_AUTO_SAMPLING','GETDATA']}
        reply = yield self.driver_client.execute(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        for sample in result:
            self.assertIsInstance(sample.get('temperature',None),float)
            self.assertIsInstance(sample.get('salinity',None),float)
            self.assertIsInstance(sample.get('sound velocity',None),float)
            self.assertIsInstance(sample.get('pressure',None),float)
            self.assertIsInstance(sample.get('conductivity',None),float)
            self.assertIsInstance(sample.get('time',None),tuple)
            self.assertIsInstance(sample.get('date',None),tuple)
        self.assertEqual(current_state,'STATE_CONNECTED')


        # Dissolve the connection to the device.
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assertEqual(success[0],'OK')
        self.assertEqual(result,None)
        self.assertEqual(current_state,'STATE_DISCONNECTED')
        
        



