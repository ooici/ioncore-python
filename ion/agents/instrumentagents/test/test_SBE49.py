#!/usr/bin/env python
"""
This module has test cases to test out SeaBird SBE49 instrument software
including the driver
@author Steve Foley
@file ion/agents/test/test_SBE49.py
"""
import logging
from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriverClient
from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriver
from ion.core import bootstrap

from magnet.spawnable import Receiver
from magnet.spawnable import spawn
from ion.core.base_process import BaseProcess

import ion.util.procutils as pu
from subprocess import Popen, PIPE
import os

class TestSBE49(IonTestCase):
    
    
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
        simDir = cwd.replace("_trial_temp", "ion/agents/instrumentagents/test/")
        #simPath = simDir("sim_SBE49.py")
        simPath = simDir + "sim_SBE49.py"
        #logPath = simDir.append("sim.log")
        logPath = simDir + "sim.log"
        logging.info("cwd: %s, simPath: %s, logPath: %s" %(str(cwd), str(simPath), str(logPath)))
        simLogObj = open(logPath, 'a')
        #self.simProc = Popen(simPath, stdout=PIPE)
        self.simProc = Popen(simPath, stdout=simLogObj)

        # Sleep for a while to allow simlator to get set up.
        yield pu.asleep(2)

        self.sup = yield bootstrap.create_supervisor()
        self.driver = SBE49InstrumentDriver()
        self.driver_pid = yield self.driver.spawn()
        yield self.driver.init()
        self.driver_client = SBE49InstrumentDriverClient(proc=self.sup,
                                                         target=self.driver_pid)
        
    @defer.inlineCallbacks
    def tearDown(self):
        logging.info("Stopping instrument simulator.")
        self.simProc.terminate()
        yield self._stop_container()
        
    @defer.inlineCallbacks
    def test_driver_load(self):
        config_vals = {'addr':'127.0.0.1', 'port':'9000'}
        result = yield self.driver_client.configure_driver(config_vals)
        self.assertEqual(result['status'], 'OK')
        self.assertEqual(result['addr'], config_vals['addr'])
        self.assertEqual(result['port'], config_vals['port'])

        
    @defer.inlineCallbacks
    def test_fetch_set(self):
        params = {'baudrate':'19200', 'outputsal':'N'}
        result = yield self.driver_client.fetch_params(params.keys())
        self.assertNotEqual(params, result)
        result = yield self.driver_client.set_params({})
        self.assertEqual(len(result.keys()), 1)
        self.assertEqual(result['status'], 'OK')
        set_result = yield self.driver_client.set_params(params)
        self.assertEqual(set_result['status'], 'OK')
        self.assertEqual(set_result['baudrate'], params['baudrate'])
        self.assertEqual(set_result['outputsal'], params['outputsal'])
        result = yield self.driver_client.fetch_params(params.keys())
        self.assertEqual(result['status'], 'OK')
        self.assertEqual(result['baudrate'], params['baudrate'])
        self.assertEqual(result['outputsal'], params['outputsal'])
        
    @defer.inlineCallbacks
    def test_execute(self):
        """
        Lame test since this doesnt do much
        """
        cmd1 = {'start': ['now']}
        cmd2 = {'stop':['now']}
        #cmd2 = {'pumpoff':['3600', '1']}
        result = yield self.driver_client.execute(cmd1)
        self.assertEqual(result['status'], 'OK')
        result = yield self.driver_client.execute(cmd2)
        self.assertEqual(result['status'], 'OK')

        # DHE: wait a while...
        yield pu.asleep(4)

        # DHE: disconnecting; a connect would probably be good.
        result = yield self.driver_client.disconnect(['some arg'])
        
        
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



        
        