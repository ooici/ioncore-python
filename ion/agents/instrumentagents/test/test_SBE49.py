#!/usr/bin/env python
"""
@file ion/agents/test/test_SBE49.py
@brief This module has test cases to test out SeaBird SBE49 instrument software
    including the driver. This assumes that generic InstrumentAgent code has
    been tested by another test case
@author Steve Foley
@see ion.agents.instrumentagents.test.test_instrument
"""
import logging
from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriverClient
from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriver
from ion.core import bootstrap

class TestSBE49(IonTestCase):


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        self.sup = yield bootstrap.create_supervisor()
        self.driver = SBE49InstrumentDriver()
        self.driver_pid = yield self.driver.spawn()
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
        cmd2 = {'pumpoff':['3600', '1']}
        result = yield self.driver_client.execute(cmd1)
        self.assertEqual(result['status'], 'OK')
        result = yield self.driver_client.execute(cmd2)
        self.assertEqual(result['status'], 'OK')
