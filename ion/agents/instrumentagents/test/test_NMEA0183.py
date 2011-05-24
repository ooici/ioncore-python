#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/test/test_NMEA0183.py
@brief Test cases for the NMEA0183 driver.
@author Alon Yaari
"""

import re
import os
import time
from twisted.internet import defer
from twisted.trial import unittest
import ion.util.ionlog
import ion.util.procutils as pu
from ion.test.iontest import IonTestCase
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceDriverClient
from ion.agents.instrumentagents.driver_NMEA0183 import DriverException
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceState
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceChannel
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceCommand
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceStatus
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceCapability
from ion.agents.instrumentagents.driver_NMEA0183 import NMEADeviceParam
from ion.agents.instrumentagents.driver_NMEA0183 \
    import NMEADeviceMetadataParameter
from ion.agents.instrumentagents.instrument_constants import InstErrorCode
from ion.agents.instrumentagents.instrument_constants import ObservatoryState
import  ion.agents.instrumentagents.simulators.sim_NMEA0183 as sim

log = ion.util.ionlog.getLogger(__name__)

def dump_dict(d, d2 = None):
    print
    for (key, val) in d.iteritems():
        if d2:
            print key, ' ', val, ' ', d2.get(key, None)
        else:
            print key, ' ', val

"""
    These tests require the following apps stored in ioncore-python:
        - bin/socat
        - bin/simGPS0183
"""


# It is useful to be able to easily turn tests on and off during development.
# Also this will ensure tests do not run automatically.
SKIP_TESTS = [
    #'test_configure',
    #'test_connect',
    'test_get_set',
    'test_get_metadata',
    'test_get_status',
    'test_get_capabilities',
    'test_execute',
    'test_execute_direct',
    'dummy'
]


class TestNMEADevice(IonTestCase):
    
    # Timeouts are irrelevant for this NMEA test case
    timeout = 120
    
    # The instrument ID.
    instrument_id = 'GPSsim'
    
    # Instrument and simulator configuration.

    device_port = sim.SERPORTSLAVE
    device_baud = 19200
    device_bytesize = 8
    device_parity = 'N'
    device_stopbits = 1
    device_timeout = 0
    device_xonxoff = 0
    device_rtscts = 0

    driver_config = { 'port': device_port,
                      'baudrate': device_baud,
                      'bytesize': device_bytesize,
                      'parity': device_parity,
                      'stopbits': device_stopbits,
                      'timeout': device_timeout,
                      'xonxoff': device_xonxoff,
                      'rtscts': device_rtscts }

    @defer.inlineCallbacks
    def setUp (self):

        print '\n          ----- setUp -----'
        yield self._start_container()

        """
        The simulator (simGPS0183) is run ON THE SAME MACHINE as the trial test.
        - Verify that the simulator app is in ioncore-python/bin/simGPS0183app
        - Verify that socat is in ioncore-python/bin/socat
        """

        print '          ----- Launching simulator...'
        self.sim = sim.NMEA0183Simulator()
        time.sleep (5)
        print '          ----- Simulator launched.'
        self.assertEqual(self.sim.IsSimulatorRunning(), 1)

        services = [{'name': 'driver_NMEA0183',
                     'module': 'ion.agents.instrumentagents.driver_NMEA0183',
                     'class': 'NMEADeviceDriver',
                     'spawnargs': {}}]

        self.sup = yield self._spawn_processes(services)
        self.driver_pid = yield self.sup.get_child_id('driver_NMEA0183')
        self.driver_client = NMEADeviceDriverClient(proc = self.sup,
                                                    target = self.driver_pid)

    @defer.inlineCallbacks
    def tearDown(self):

        print '\n          ----- tearDown -----'

        yield self.sim.StopSimulator()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_configure (self):
        """
        Test driver configure functions.
        """

        print '\n          ----- test_configure -----'

        if 'test_configure' in SKIP_TESTS:
            raise unittest.SkipTest('Test is implicitly marked for skip.')

        params = self.driver_config

        # We should begin in the unconfigured state.
        print '          ----- Begin in unconfigured state'
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state, NMEADeviceState.UNCONFIGURED)
        print '          ----- Driver started in unconfigured state'

        # Configure the driver and verify.
        print '          ----- Configure the driver'
        reply = yield self.driver_client.configure (params)
        print '          ----- Configuration sent to driver'
        current_state = yield self.driver_client.get_state()
        print '          ----- Now in %s state' %current_state
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result, params)
        self.assertEqual(current_state, NMEADeviceState.DISCONNECTED)

    
    @defer.inlineCallbacks
    def test_connect(self):
        """
        Test driver connect to device.
        """

        print '\n          ----- test_connect -----'

        if 'test_connect' in SKIP_TESTS:
            raise unittest.SkipTest('Test is implicitly marked for skip.')

        params = self.driver_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual(current_state, NMEADeviceState.UNCONFIGURED)

        # Configure the driver and verify.
        reply = yield self.driver_client.configure(params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_(InstErrorCode.is_ok (success))
        self.assertEqual(result, params)
        self.assertEqual(current_state, NMEADeviceState.DISCONNECTED)

        # Establish connection to device and verify.

        print '\n\n\n          ----- Establish connection'
        reply = yield self.driver_client.connect()
#        except Exception, e:
#            print '          ----- Exception: %s' % e
#            self.fail               ('Could not connect to the device.')

        print '\n\n\n          ----- Get State after establishing connection'
        current_state = yield self.driver_client.get_state()
        print '\n\n\n          ----- Got State: %s' % current_state
        success = reply['success']
        result = reply['result']

        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result,None)
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Dissolve the connection to the device.
        print '\n\n\n          ----- Disconnect'
        reply = yield self.driver_client.disconnect()
        print '\n\n\n          ----- Get State after disconnecting'
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)
        

        