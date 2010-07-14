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

from ion.agents.instrumentagents.SBE49 import SBE49InstrumentDriverClient
from ion.agents.instrumentagents.SBE49 import SBE49InstrumentDriver
from ion.core import bootstrap

class TestSBE49(IonTestCase):
    
    
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
#        processes = [{'name':'testSBE49driver',
#              'module':'ion.agents.instrumentagents.SBE49',
#              'class':'SBE49InstrumentDriver'}]
#        self.sup = yield self._spawn_processes(processes)
        self.sup = yield bootstrap.create_supervisor()
        self.driver = SBE49InstrumentDriver()
        self.driver_pid = yield self.driver.spawn()
        yield self.driver.init()
        logging.debug("*** sup: %s, driver pid: %s", self.sup, self.driver_pid)
        self.driver_client = SBE49InstrumentDriverClient(proc=self.sup,
                                                         target=self.driver_pid)
        
    @defer.inlineCallbacks
    def tearDown(self):
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
        self.assertEqual(result['status'], 'ERROR')
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
        cmd1 = ['start', 'now']
        cmd2 = ['pumpoff', '3600', '1']
        result = yield self.driver_client.execute(cmd1)
        self.assertEqual(result['status'], 'OK')
        self.assertEqual(result['value'], cmd1[0])
        result = yield self.driver_client.execute(cmd2)
        self.assertEqual(result['status'], 'OK')
        self.assertEqual(result['value'], cmd2[0])
