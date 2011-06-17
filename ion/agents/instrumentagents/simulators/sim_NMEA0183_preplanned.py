#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/simulators/sim_NMEA0183_preplanned.py
@brief Launches a simulated GPS that outputs NMEA0183, sourced from a predefined route.
@author Alon Yaari
"""


from datetime import datetime
from twisted.internet import task, reactor
from twisted.internet.serialport import SerialPort
import sim_NMEA0183
from gpsSimPath import simPath
from twisted.protocols import basic
import ion.agents.instrumentagents.helper_NMEA0183 as NMEA

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

OFF = 'Off'
ON = 'On'


class NMEASimProtocol (basic.LineReceiver):
    """
    """
    delimiter = '\r'
    def lineReceived (self, line):
        log.info(line)
        SimWriteToSerial (line)
        inNMEA = NMEA.NMEAInString (line)
        inData = inNMEA.GetNMEAInData()
        if type (inData) == type ({}):
            log.debug ('Type was dict')
            if inData.has_key ('NMEA_CD'):
                log.debug ('has_key')
                if inData['NMEA_CD'] == 'PGRMO':
                    self.SetSentenceStatus (inData)
            else:
                log.debug ('Does not have_key')
        else:
            log.debug ('Was not dict, was %s' % type (inData))

    def SetSentenceStatus (self, d):
        """
        0     Turn off specified sentence
        1     Turn on specified sentence
        2     Disable all sentences
        3     Enable all sentences (except GPALM if available)
        4     Supposed to reset factory, here same as 3
        """
        mode = d['PGRMODE']
        target = d['TARGET']
        log.debug ('target: %s    mode: %s' % (mode, target))
        new = []
        if mode > 2:
            for sen in NMEA0183SimPrePlanned.SentenceStatus:
                new.append ({'Type': sen['Type'], 'Status': ON})
            log.info ('PGRMO statement enabled all NMEA output.')
        elif mode == 2:
            for sen in NMEA0183SimPrePlanned.SentenceStatus:
                new.append ({'Type': sen['Type'], 'Status': OFF})
            log.info ('PGRMO statement disabled all NMEA output.')
        elif mode == 1:
            for sen in NMEA0183SimPrePlanned.SentenceStatus:
                if sen['Type'] == target:
                    new.append ({'Type': target, 'Status': ON})
                else:
                    new.append ({'Type': sen['Type'], 'Status': sen['Status']})
            log.info ('PGRMO statement enabled NMEA statement %s' % target)
        elif mode == 0:
            for sen in NMEA0183SimPrePlanned.SentenceStatus:
                if sen['Type'] == target:
                    new.append ({'Type': target, 'Status': OFF})
                else:
                    new.append ({'Type': sen['Type'], 'Status': sen['Status']})
            log.info ('PGRMO statement disabled NMEA statement %s' % target)
        else:
            log.info ('PGRMO statement has no effect.')
        if len (new) > 0:
            NMEA0183SimPrePlanned.SentenceStatus = new

def DebugShowSentenceStatus():
    for sentence in NMEA0183SimPrePlanned.SentenceStatus:
        log.info ('         %s: %s' % (sentence['Type'], sentence['Status']))

def SimWriteToSerial (toWrite):
        """
        """
        if NMEA0183SimPrePlanned.s:
            NMEA0183SimPrePlanned.s.write (toWrite)
            log.info ('NMEA written: %s' % toWrite)

class NMEA0183SimPrePlanned (sim_NMEA0183.NMEA0183SimBase):
    """
    Provide a simulated NMEA0183 GPS through a local virtual serial port.
    """

    WHICHSIM = 'NMEA0183 GPS Preplanned Route Simulator'

    SentenceStatus = [{'Type': 'GPGGA', 'Status': 'Off'},
                      {'Type': 'GPRMC', 'Status': 'Off'}]

    s = None

    def __init__ (self):
        log.info ('Preplanned __init__')
        sim_NMEA0183.NMEA0183SimBase.__init__ (self)

    def SimGPSSetup (self):
        """
        Initialize preplanned route GPS simulator
        @param None
        @retval None
        """

        log.info ('Setup: Preplanned NMEA0183 Simulator')
        
        self._workingSim = False
        if not self._goodComms:
            log.info ('Could not start NMEA0183 simulator: no serial comms.')
            return

        # Connect to the master serial port
        log.info ('Opening %s for simulator to connect with.' % self._serMaster)
        baud = 57600
        try:
            self.s = SerialPort (NMEASimProtocol(),
                                 self._serMaster,
                                 reactor,
                                 baudrate = 57600)
        except Exception, e:
            log.info ('Failure: ' + e.__str__())
            return
        log.info ('Successfuly opened  %s.' % self._serMaster)
        #asleep (1)
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
        if self._refSecs > len (simPath):
            self._refSecs == 0

        lat = simPath[self._refSecs]['lat']
        lon = simPath[self._refSecs]['lon']
        cog = simPath[self._refSecs]['cog']
        sog = simPath[self._refSecs]['sog']
        curTime = datetime.utcnow()
        n = {'lat': lat,
             'lon': lon,
             'time': curTime,
             'sog': '%3.1f' % sog,
             'cog': '%3.1f' % cog}
        written = 0
        for sen in NMEA0183SimPrePlanned.SentenceStatus:
            if sen['Status'] == ON:
                if sen['Type'] == 'GPGGA':
                    nmeaOut = sim_NMEA0183.BuildGPGGA (n)
                    self.SimWriteToSerial (nmeaOut)
                    written += 1
                elif sen['Type'] == 'GPRMC':
                    nmeaOut = sim_NMEA0183.BuildGPRMC (n)
                    self.SimWriteToSerial (nmeaOut)
                    written += 1
        log.info ('Wrote %d NMEA sentences' % written)
        # DebugShowSentenceStatus()

    def SimWriteToSerial (self, toWrite):
        """
        """
        if not self.IsSimOK():
            self._workingSim = False
            log.info ('NMEA not written: no working simulator')
            return
        if not self.IsSocatRunning():
            self._goodComms = False
            log.info ('NMEA not written: socat not running')
            return
        self.s.write (toWrite)
        log.info ('NMEA written: %s' % toWrite)

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

