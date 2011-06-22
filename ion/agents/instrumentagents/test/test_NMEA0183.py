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
import ion.agents.instrumentagents.helper_NMEA0183 as NMEA
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
from ion.agents.instrumentagents.simulators.sim_NMEA0183 \
    import SERPORTSLAVE


import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# SELECT ONE SIMULATOR
#   - Comment out the simulator not being used

#from ion.agents.instrumentagents.simulators.sim_NMEA0183_liveSFBay \
#   import NMEA0183SimliveSFBay as sim
#log.info ('Using SF BAY LIVE AIS DATA simulator')

from ion.agents.instrumentagents.simulators.sim_NMEA0183_preplanned \
    import NMEA0183SimPrePlanned as sim
log.info ('Using PREPLANNED ROUTE GPS Simulator')

log = ion.util.ionlog.getLogger(__name__)

# It is useful to be able to easily turn tests on and off
# during development. Also this will ensure tests do not run
# automatically.
SKIP_TESTS = [
    #'test_NMEAParser',
    'test_configure',
    'test_connect'
]

def dump_dict (d, d2 = None):
    print
    for (key, val) in d.iteritems():
        if d2:
            print key, ' ', val, ' ', d2.get(key, None)
        else:
            print key, ' ', val

"""
Simulator dependencies required:
    LIVESFBAY:
        - socat
        - simGPS0183

    PREPLANNED:
        - socat
"""

class TestNMEADevice(IonTestCase):
    """
    """

    # Timeouts are irrelevant for this NMEA test case
    timeout = 120
    
    # The instrument ID.
    instrument_id = 'GPSsim'
    
    # Instrument and simulator configuration.

    device_port = SERPORTSLAVE
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
        """
        Prepare container and simulator for testing
        """
        yield self._start_container()
        self._sim = sim()
        self.assertEqual (self._sim.IsSimulatorRunning(), 1)

        services = [{'name': 'driver_NMEA0183',
                     'module': 'ion.agents.instrumentagents.driver_NMEA0183',
                     'class': 'NMEADeviceDriver',
                     'spawnargs': {}}]

        self.sup = yield self._spawn_processes (services)
        self.driver_pid = yield self.sup.get_child_id ('driver_NMEA0183')
        self.driver_client = NMEADeviceDriverClient (proc = self.sup, target = self.driver_pid)

    @defer.inlineCallbacks
    def tearDown (self):
        """
        """
        yield self._sim.StopSimulator()
        yield self._stop_container()

    def test_NMEAParser (self):
        """
        Verify NMEA parsing routines.
        """
        if 'test_NMEAParser' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        # Verify parsing of known VALID GPGGA string
        testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*52'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())

        # Verify parsing of known INVALID GPGGA string (has bad checksum)
        testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*F2'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())

        # Verify parsing of known VALID dummy string
        testNMEA = '$XXXXX,0'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())

        # Verify line endings: <LF>, <CR>, <CR><LF>, and <LF><CR>
        testNMEA = '$XXXXX,0\r'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())
        testNMEA = '$XXXXX,0\n'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())
        testNMEA = '$XXXXX,0\r\n'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())
        testNMEA = '$XXXXX,0\n\r'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())

        # Verify parsing of known VALID GPRMC string with checksum
        testNMEA = '$GPRMC,225446,A,4916.45,N,12311.12,W,000.5,054.7,191194,020.3,E*68'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())

        # Verify parsing of known VALID GPRMC string without checksum
        testNMEA = '$GPRMC,225446,A,4916.45,N,12311.12,W,000.5,054.7,191194,020.3,E'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())

        # Verify parsing of known INVVALID GPRMC (not enough fields)
        testNMEA = '$GPRMC,225446,A,4916.45,N,12311.12,W,000.5'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())

        # Verify reporting of status (PGRMC command)
        testNMEA = '$PGRMC'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue (parseNMEA.IsValid())

    @defer.inlineCallbacks
    def test_configure (self):
        """
        Test driver configure functions.
        """

        if 'test_configure' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        params = self.driver_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual (current_state, NMEADeviceState.UNCONFIGURED)

        # Configure the driver and verify.
        log.debug ('Configure the driver')
        reply = yield self.driver_client.configure (params)
        log.debug ('Configuration sent to driver')
        current_state = yield self.driver_client.get_state()
        log.debug ('Now in %s state' %current_state)
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, params)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

    @defer.inlineCallbacks
    def test_connect (self):
        """
        Test driver connect to device.
        """

        if 'test_connect' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping during development.')

        params = self.driver_config

        # We should begin in the unconfigured state.
        current_state = yield self.driver_client.get_state()
        self.assertEqual (current_state, NMEADeviceState.UNCONFIGURED)

        # Configure the driver and verify.
        reply = yield self.driver_client.configure (params)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, params)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

        # Establish connection to device and verify.
        log.info ('----- Establish connection')
        try:
            reply = yield self.driver_client.connect()
        except Exception, ex:
            self.fail('Could not connect to the device.')
            
        log.info ('----- Get State after establishing connection')
        current_state = yield self.driver_client.get_state()
        log.info ('----- Got State: %s, reply: %s', current_state, reply)
        
        self.assert_ (InstErrorCode.is_ok (reply['success']))
        self.assertEqual (reply['result'], None)
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Dissolve the connection to the device.
        log.info ('----- Disconnect')
        reply = yield self.driver_client.disconnect()
        log.info ('----- Get State after disconnecting')
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual(result, None)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)
        

        