#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/simulators/sim_NMEA0183_preplanned.py
@brief Launches a simulated GPS that outputs NMEA0183, sourced from a predefined route.
@author Alon Yaari
"""


from datetime import datetime
from twisted.internet import task, reactor
from twisted.internet.serialport import SerialPort
from sim_NMEA0183 import *
from gpsSimPath import simPath
from twisted.protocols import basic

GPSDEBUG = True     # Set true for debug output
PREPLAN_MAX = 500


class NMEASimProtocol (basic.LineReceiver):
    """
    """

    def lineReceived (self, line):
        NMEA0183SimPrePlanned.SimWriteToSerial (line)


class NMEA0183SimPrePlanned (NMEA0183SimBase):
    """
    Provide a simulated NMEA0183 GPS through a local virtual serial port.
    """

    WHICHSIM = 'NMEA0183 GPS Preplanned Route Simulator'

    def SimGPSSetup(self):
        """
        Initialize preplanned route GPS simulator
        @param None
        @retval None
        """

        GPSDebug ('Setup: Preplanned NMEA0183 Simulator')
        
        self._workingSim = False
        if not self._goodComms:
            GPSDebug('Could not start NMEA0183 simulator: no serial comms.')
            return

        # Connect to the master serial port
        GPSDebug ('Opening %s for simulator to connect with.' % self._serMaster)
        baud = 57600
        try:

            self._s = SerialPort (NMEASimProtocol(),
                                  self._serMaster,
                                  reactor,
                                  baudrate = 57600)
        except Exception, e:
            GPSDebug ('Failure: ' + e.__str__())
            return
        GPSDebug ('Successfuly opened  %s.' % self._serMaster)
        time.sleep (1)
        self._workingSim = True
        self._goodComms = True

        # Launch a reactor task to generate NMEA output each second
        self._refSecs = -1
        self._simRun = task.LoopingCall (self.NMEAOutput)
        self._simRun.start (1.0)
        reactor.run()

    def NMEAOutput(self):
        """
        Called each second to produce NMEA output.
            - Looks up values in a predefined table
        """

        # Manage the index
        self._refSecs += 1
        if self._refSecs > PREPLAN_MAX:
            self._refSecs == 0

        lat = simPath[self._refSecs]['lat']
        lon = simPath[self._refSecs]['lon']
        curTime = datetime.utcnow()
        nmeaOut = BuildGPGGA ({'lat': lat, 'lon': lon, 'time': curTime})
        self.SimWriteToSerial (nmeaOut)

    def SimWriteToSerial(self, toWrite):
        """
        """
        if not self.IsSimOK():
            self._workingSim = False
            GPSDebug ('NMEA not written: no working simulator')
            return
        if not self.IsSocatRunning():
            self._goodComms = False
            GPSDebug ('NMEA not written: socat not running')
            return
        self._s.write (toWrite)
        GPSDebug ('NMEA written: %s' % toWrite)

    def SimShutdown(self):
        """
        Stop the GPS simulator from firing every second.
        """

        self._simRun.stop()
        # reactor.run()
        self._workingSim = False

    def IsSimulatorRunning(self):
        return self._workingSim




#

