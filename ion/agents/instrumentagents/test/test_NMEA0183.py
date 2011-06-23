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
    import SERPORTSLAVE, OFF, ON


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
    'test_NMEAParser',
    # 'test_configure',
    'test_connect',
    'test_get_set'
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
    bogus_config  = { 'port': 'NOPORT',
                      'baudrate': -1,
                      'bytesize': 12,
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
            raise unittest.SkipTest ('Skipping test_NMEAParser during development.')

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

        log.debug ('test_NMEAParser complete')

    @defer.inlineCallbacks
    def test_configure (self):
        """
        Test driver configure functions.
        """
        if 'test_configure' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping test_configure during development.')

        # We should begin in the unconfigured state.
        log.debug ('Verifying driver is UNCONFIGURED state')
        current_state = yield self.driver_client.get_state()
        self.assertEqual (current_state, NMEADeviceState.UNCONFIGURED)

        # Configure the driver properly and verify that we're in DISCONNECTED state.
        log.debug ('Configuring driver correctly then verifying driver is in DISCONNECTED state')
        reply = yield self.driver_client.configure (self.driver_config)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, self.driver_config)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

        # Configure the driver improperly so that it fails to configure
        log.debug ('Configuring driver incorrectly, verifying bad config and driver is still in DISCONNECTED state')
        reply = yield self.driver_client.configure (self.bogus_config)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_error (success))
        self.assertEqual (result, self.bogus_config)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

        log.debug ('test_configure complete')

    @defer.inlineCallbacks
    def test_connect (self):
        """
        Test driver connect to device.
        """
        if 'test_connect' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping test_connect during development.')

        # We should begin in the unconfigured state.
        log.debug ('Verifying driver is UNCONFIGURED state')
        current_state = yield self.driver_client.get_state()
        self.assertEqual (current_state, NMEADeviceState.UNCONFIGURED)

        # Try to connect to the device without configuring first (should fail)
        log.debug ('Verifying that driver cannot connect to device in UNCONFIGURED state')
        reply = yield self.driver_client.connect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_error (success))
        self.assertEqual (result, None)
        self.assertEqual (current_state, NMEADeviceState.UNCONFIGURED)

        # Configure the driver properly and verify that we're in DISCONNECTED state.
        log.debug ('Configuring driver correctly then verifying driver is in DISCONNECTED state')
        reply = yield self.driver_client.configure (self.driver_config)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, self.driver_config)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

        # Establish connection to the device and verify that we're in CONNECTED state..
        log.debug ('Connecting to the device then verifying driver is in CONNECTED state')
        reply = yield self.driver_client.connect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, None)
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Verify that since driver is connected, connection cannot happen again (should fail)
        log.debug ('Verifying driver cannot connect again to a connected device')
        reply = yield self.driver_client.connect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_error (success))
        self.assertEqual (result, None)
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Dissolve the connection to the device.
        log.debug ('Disconnecting from device and verifying back in DISCONNECTED state')
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual(result, None)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

        log.debug ('test_configure complete')

    @defer.inlineCallbacks
    def test_get_set (self):
        """
        Test driver get/set methods with device.
        """
        if 'test_connect' in SKIP_TESTS:
            raise unittest.SkipTest('Skipping test_connect during development.')

        # We should begin in the unconfigured state.
        log.debug ('Verifying driver is UNCONFIGURED state')
        current_state = yield self.driver_client.get_state()
        self.assertEqual (current_state, NMEADeviceState.UNCONFIGURED)

        # Configure the driver properly and verify that we're in DISCONNECTED state.
        log.debug ('Configuring driver correctly then verifying driver is in DISCONNECTED state')
        reply = yield self.driver_client.configure (self.driver_config)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, self.driver_config)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

        # Establish connection to the device and verify that we're in CONNECTED state..
        log.debug ('Connecting to the device then verifying driver is in CONNECTED state')
        reply = yield self.driver_client.connect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, None)
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Get all parameters and verify that the ones being reported are what we expect
        log.debug ('Getting and verifying expected parameters from the device')
        timeout = 30
        params = [(NMEADeviceChannel.GPS, NMEADeviceParam.ALL)]
        reply = yield self.driver_client.get (params, timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        gpsParams = { (NMEADeviceChannel.GPS, 'GPGGA'),
                      (NMEADeviceChannel.GPS, 'GPGLL'),
                      (NMEADeviceChannel.GPS, 'GPRMC'),
                      (NMEADeviceChannel.GPS, 'PGRMF'),
                      (NMEADeviceChannel.GPS, 'PGRMC'),
                      (NMEADeviceChannel.GPS, 'FIX_MODE'),
                      (NMEADeviceChannel.GPS, 'ALT_MSL'),
                      (NMEADeviceChannel.GPS, 'E_DATUM'),
                      (NMEADeviceChannel.GPS, 'DIFFMODE'),
                      (NMEADeviceChannel.GPS, 'BAUD_RT'),
                      (NMEADeviceChannel.GPS, 'MP_OUT'),
                      (NMEADeviceChannel.GPS, 'MP_LEN'),
                      (NMEADeviceChannel.GPS, 'DED_REC')}
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (gpsParams.sort(), result.keys().sort())
        self.assertEqual (all (map (lambda x: x[1] != None, result.values())), True)
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Use set to turn off all sentences
        newParams = {}
        newParams[NMEADeviceChannel.GPS, 'GPGGA'] = OFF
        newParams[NMEADeviceChannel.GPS, 'GPGLL'] = OFF
        newParams[NMEADeviceChannel.GPS, 'GPRMC'] = OFF
        newParams[NMEADeviceChannel.GPS, 'PGRMF'] = OFF
        newParams[NMEADeviceChannel.GPS, 'PGRMC'] = OFF
        reply = yield self.driver_client.set (newParams, timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok(success))
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Get parameters to verify that sentences are off
        log.debug ('Shutting off all sentences and verifing they are off')
        reply = yield self.driver_client.get (params, timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'GPGGA')], OFF)
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'GPGLL')], OFF)
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'GPRMC')], OFF)
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'PGRMF')], OFF)
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'PGRMC')], OFF)
        self.assertEqual (current_state,NMEADeviceState.CONNECTED)

        # Use set to turn on all sentences
        newParams = {}
        newParams[NMEADeviceChannel.GPS, 'GPGGA'] = ON
        newParams[NMEADeviceChannel.GPS, 'GPGLL'] = ON
        newParams[NMEADeviceChannel.GPS, 'GPRMC'] = ON
        newParams[NMEADeviceChannel.GPS, 'PGRMF'] = ON
        newParams[NMEADeviceChannel.GPS, 'PGRMC'] = ON
        reply = yield self.driver_client.set (newParams, timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok(success))
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Get parameters to verify that sentences are on
        log.debug ('Turning on all sentences and verifing they are on')
        reply = yield self.driver_client.get (params, timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'GPGGA')], ON)
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'GPGLL')], ON)
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'GPRMC')], ON)
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'PGRMF')], ON)
        self.assertEqual (result[(NMEADeviceChannel.GPS, 'PGRMC')], ON)
        self.assertEqual (current_state,NMEADeviceState.CONNECTED)

        # Try getting a mix of good and bad parameters
        log.debug ('Testing get of implicitly named good and bad parameters')
        params = [
            ('BOGUS Channel Name', 'GPGGA'),
            (NMEADeviceChannel.GPS, 'GPGGA'),
            (NMEADeviceChannel.GPS, 'BOGUS') ]
        reply = yield self.driver_client.get (params, timeout)
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assert_ (InstErrorCode.is_error (result[('BOGUS Channel Name', 'GPGGA')][0]))
        self.assertEqual (result [('BOGUS Channel Name', 'GPGGA')][1], None)
        self.assert_ (InstErrorCode.is_ok (result[(NMEADeviceChannel.GPS, 'GPGGA')][0]))
        self.assert_ (InstErrorCode.is_error (result[(NMEADeviceChannel.GPS, 'BOGUS')][0]))
        self.assertEqual (current_state,NMEADeviceState.CONNECTED)

        # TODO: Try setting mix of good and bad parameters

        # Dissolve the connection to the device.
        log.debug ('Disconnecting from device and verifying back in DISCONNECTED state')
        reply = yield self.driver_client.disconnect()
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual(result, None)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

        log.debug ('test_get_set complete')

