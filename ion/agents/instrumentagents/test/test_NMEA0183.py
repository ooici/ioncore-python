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
<<<<<<< HEAD
=======
        raise unittest.SkipTest('Not working yet')

        print '\n          ----- setUp -----'
        yield self._start_container()

>>>>>>> mainline_dev
        """
        Prepare container and simulator for testing
        """

        log.info ('\n----- Starting setUp() -----')
        yield self._start_container()

        log.info ('----- Launching simulator:  ' + sim.WHICHSIM)
        self._sim = sim()
        if self._sim.IsSimOK():
            log.info ('----- Simulator launched.')
        self.assertEqual (self._sim.IsSimulatorRunning(), 1)

        services = [{'name': 'driver_NMEA0183',
                     'module': 'ion.agents.instrumentagents.driver_NMEA0183',
                     'class': 'NMEADeviceDriver',
                     'spawnargs': {}}]

        self.sup = yield self._spawn_processes (services)
        self.driver_pid = yield self.sup.get_child_id ('driver_NMEA0183')
        self.driver_client = NMEADeviceDriverClient (proc = self.sup,
                                                     target = self.driver_pid)

    @defer.inlineCallbacks
    def tearDown (self):
        """
        """

        log.info ('----- tearDown -----')
        yield self._sim.StopSimulator()
        yield self._stop_container()

    def test_NMEAParser (self):
        """
        Verify NMEA parsing routines.
        """
        # Completely valid GPGGA string
        log.info ('Verify NMEA parsing with known good GPGGA sentence:')
        testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*52'
        parseNMEA = NMEA.NMEAString (testNMEA)
        self.assertTrue(parseNMEA.IsValid())
        
        # Completely valid dummy string
        log.info ('Verify NMEA parsing with defined dummy setence:')
        testNMEA = '$XXXXX,0'
        parseNMEA = NMEA.NMEAString(testNMEA)
        self.assertTrue(parseNMEA.IsValid())
        
        # Invalid GPGGA string checksum
        log.info ('Verify correct NMEA behavior when passed a bad NMEA checksum:')
        testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*F2'
        parseNMEA = NMEA.NMEAString(testNMEA)
        self.assertTrue(parseNMEA.IsValid())
        
    @defer.inlineCallbacks
    def test_configure (self):
        """
        Test driver configure functions.
        """
        params = self.driver_config

        # We should begin in the unconfigured state.
        log.info ('----- Begin in unconfigured state')
        current_state = yield self.driver_client.get_state()
        self.assertEqual (current_state, NMEADeviceState.UNCONFIGURED)
        log.info ('----- Driver started in unconfigured state')

        # Configure the driver and verify.
        log.info        ('----- Configure the driver')
        reply           = yield self.driver_client.configure (params)
        log.info        ('----- Configuration sent to driver')
        current_state   = yield self.driver_client.get_state()
        log.info        ('----- Now in %s state' %current_state)
        success         = reply['success']
        result          = reply['result']
        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, params)
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)

    @defer.inlineCallbacks
    def test_connect (self):
        """
        Test driver connect to device.
        """
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
        reply = yield self.driver_client.connect()

        log.info ('----- Get State after establishing connection')
        current_state = yield self.driver_client.get_state()
        log.info ('----- Got State: %s' % current_state)
        success = reply['success']
        result = reply['result']

        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (result, None)
        self.assertEqual (current_state, NMEADeviceState.CONNECTED)

        # Dissolve the connection to the device.
        log.info ('----- Disconnect')
        reply = yield self.driver_client.disconnect()
        log.info ('----- Get State after disconnecting')
        current_state = yield self.driver_client.get_state()
        success = reply['success']
        result = reply['result']

        self.assert_ (InstErrorCode.is_ok (success))
        self.assertEqual (current_state, NMEADeviceState.DISCONNECTED)
        

        