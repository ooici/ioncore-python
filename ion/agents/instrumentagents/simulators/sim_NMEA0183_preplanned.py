#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/simulators/sim_NMEA0183_preplanned.py
@brief Launches a simulated GPS that outputs NMEA0183, sourced from a predefined route.
@author Alon Yaari
"""

"""
    Commands accepted by the simulator

    $PGRMC...           Any valid PGRMC set command
    $PGRMC              All valid and invalid PGRMC results in return of $PGRMC sentence with current settings
    $PGRMO,,3           Enables all sentences
    $PGRMO,,4           Enables all sentences
    $PGRMO,,2           Disables all sentences
    $PGRMO,GPGGA,0      Disables GPGGA sentence
    $PGRMO,GPRMC,0      Disables GPRMC sentence
    $PGRMO,OOIXX,0      Disables OOIXX sentence
    $PGRMO,GPGGA,1      Enables GPGGA sentence
    $PGRMO,GPRMC,1      Enables GPRMC sentence
    $PGRMO,OOIXX,1      Enables OOIXX sentence

    - No spaces allowed in a command sentence
    - Terminate sentence with '\r'
"""

from datetime import datetime
from twisted.internet import task, reactor, defer
from twisted.internet.serialport import SerialPort
# import sim_NMEA0183
# from gpsSimPath import simPath


import ion.agents.instrumentagents.simulators.sim_NMEA0183 as sim_NMEA0183
from ion.agents.instrumentagents.simulators.gpsSimPath import simPath
from twisted.protocols import basic
import ion.agents.instrumentagents.helper_NMEA0183 as NMEA

import ion.util.procutils as pu
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

PATHLEN = len(simPath) - 1

class NMEASimProtocol(basic.LineReceiver):
    """
    """
    delimiter = '\r'
    def lineReceived(self, line):
        log.info(line)
        NMEA0183SimPrePlanned.pendingOutput.append(line.rstrip())
        inNMEA = NMEA.NMEAInString(line)
        inData = inNMEA.GetNMEAInData()
        if type(inData) == type({}):
            if inData.has_key('NMEA_CD'):
                if inData['NMEA_CD'] == 'PGRMO':
                    self.SetSentenceStatus(inData)
                elif inData['NMEA_CD'] == 'PGRMC':
                    if inNMEA.IsValid():
                        self.SetConfiguration(inData)
                    # Send a PGRMC string with the new values to the user
                    NMEA0183SimPrePlanned.pendingOutput.append(sim_NMEA0183.BuildPGRMC())

    def SetConfiguration(self, d):
        """
        """
        log.debug(d)
        if d.has_key('FIX_MODE') and len(d['FIX_MODE']) > 0:
            if 'A23'.find(d['FIX_MODE'][0]) > -1:
                sim_NMEA0183.NMEA0183SimBase.cfg_FIXMODE = d['FIX_MODE'][0]

        if d.has_key('ALT_MSL') and len(d['ALT_MSL']) > 0:
            dVal = float(d['ALT_MSL'])
            if dVal >= -1500.0 and dVal <= 18000.0:
                sim_NMEA0183.NMEA0183SimBase.cfg_ALT = dVal

        if d.has_key('E_DATUM') and len(d['E_DATUM']) > 0:
            iVal = int(d['E_DATUM'])
            if iVal > -1 and iVal < 110:
                sim_NMEA0183.NMEA0183SimBase.cfg_DATUMINDEX = iVal

        if d.has_key('DIFFMODE') and len(d['DIFFMODE']) > 0:
            if d['DIFFMODE'][0] == 'A' or d['DIFFMODE'][0] == 'D':
                sim_NMEA0183.NMEA0183SimBase.cfg_DIFFMODE = d['DIFFMODE'][0]

        if d.has_key('BAUD_RT') and len(d['BAUD_RT']) > 0:
            iVal = int(d['BAUD_RT'])
            if iVal > 0 and iVal < 9:
                sim_NMEA0183.NMEA0183SimBase.cfg_BAUD = iVal

        if d.has_key('VEL_FILT') and len(d['VEL_FILT']) > 0:
            iVal = int(d['VEL_FILT'])
            if iVal > -1 and iVal < 256:
                sim_NMEA0183.NMEA0183SimBase.cfg_VELFILTER = iVal

        if d.has_key('MP_OUT') and len(d['MP_OUT']) > 0:
            if d['MP_OUT'][0] == '1' or d['MP_OUT'][0] == '2':
                sim_NMEA0183.NMEA0183SimBase.cfg_MPO = d['MP_OUT'][0]

        if d.has_key('MP_LEN') and len(d['MP_LEN']) > 0:
            iVal = int(d['MP_LEN'])
            if iVal > -1 and iVal < 49:
                sim_NMEA0183.NMEA0183SimBase.cfg_MPOLEN = iVal

        if d.has_key('DED_REC') and len(d['DED_REC']) > 0:
            iVal = int(d['DED_REC'])
            if iVal > 0 and iVal < 31:
                sim_NMEA0183.NMEA0183SimBase.cfg_DEDRECKON = iVal

    def SetSentenceStatus(self, d):
        """
        0     Turn off specified sentence
        1     Turn on specified sentence
        2     Disable all sentences
        3     Enable all sentences(except GPALM if available)
        4     Supposed to reset factory, here same as 3
        """
        mode = d['PGRMODE']
        target = d['TARGET']
        log.debug('target: %s    mode: %s' %(mode, target))
        new = []
        if mode > 2:
            for sen in NMEA0183SimPrePlanned.SentenceStatus:
                new.append({'Type': sen['Type'], 'Status': sim_NMEA0183.ON})
            log.info('PGRMO statement enabled all NMEA output.')
        elif mode == 2:
            for sen in NMEA0183SimPrePlanned.SentenceStatus:
                new.append({'Type': sen['Type'], 'Status': sim_NMEA0183.OFF})
            log.info('PGRMO statement disabled all NMEA output.')
        elif mode == 1:
            for sen in NMEA0183SimPrePlanned.SentenceStatus:
                if sen['Type'] == target:
                    new.append({'Type': target, 'Status': sim_NMEA0183.ON})
                else:
                    new.append({'Type': sen['Type'], 'Status': sen['Status']})
            log.info('PGRMO statement enabled NMEA statement %s' % target)
        elif mode == 0:
            for sen in NMEA0183SimPrePlanned.SentenceStatus:
                if sen['Type'] == target:
                    new.append({'Type': target, 'Status': sim_NMEA0183.OFF})
                else:
                    new.append({'Type': sen['Type'], 'Status': sen['Status']})
            log.info('PGRMO statement disabled NMEA statement %s' % target)
        else:
            log.info('PGRMO statement has no effect.')
        if len(new) > 0:
            NMEA0183SimPrePlanned.SentenceStatus = new

def DebugShowSentenceStatus():
    for sentence in NMEA0183SimPrePlanned.SentenceStatus:
        log.info('         %s: %s' %(sentence['Type'], sentence['Status']))

def SimWriteToSerial(toWrite):
        """
        """
        if NMEA0183SimPrePlanned.s:
            NMEA0183SimPrePlanned.s.write(toWrite)
            log.info('NMEA written: %s' % toWrite)

class NMEA0183SimPrePlanned(sim_NMEA0183.NMEA0183SimBase):
    """
    Provide a simulated NMEA0183 GPS through a local virtual serial port.
    """

    WHICHSIM = 'NMEA0183 GPS Preplanned Route Simulator'
    pendingOutput = []
    SentenceStatus = [{'Type': 'GPGGA', 'Status': sim_NMEA0183.ON},
                      {'Type': 'GPRMC', 'Status': sim_NMEA0183.OFF},
                      {'Type': 'OOIXX', 'Status': sim_NMEA0183.OFF}]
    s = None

    def __init__(self):
        log.debug('Preplanned __init__')
        sim_NMEA0183.NMEA0183SimBase.__init__(self)

    @defer.inlineCallbacks
    def SimGPSSetup(self):
        """
        Initialize preplanned route GPS simulator
        @param None
        @retval None
        """
        log.debug('Setup: Preplanned NMEA0183 Simulator')
        self._workingSim = False
        if not self._goodComms:
            log.info('Could not start NMEA0183 simulator: no serial comms.')
            return

        # Connect to the master serial port
        log.debug('Opening %s for simulator to connect with.' % self._serMaster)
        try:
            self.s = SerialPort(NMEASimProtocol(),
                                 self._serMaster,
                                 reactor,
                                 baudrate = 19200)
        except Exception, e:
            log.error('GPS Simulator Failure: ' + e.__str__())
            return
        log.debug('Successfuly opened  %s.' % self._serMaster)
        yield pu.asleep(1)
        self._workingSim = True
        self._goodComms = True

        # Launch a reactor task to generate NMEA output each second
        self._refSecs = -1
        self._simRun = task.LoopingCall(self.NMEAOutput)
        self._simRun.start(1.0)
        log.info('GPS Simulator is now running at 1hz')

    def NMEAOutput(self):
        """
        Called each second to produce NMEA output.
        """

        # Write any pending output generated from outside
        while len(NMEA0183SimPrePlanned.pendingOutput) > 0:
            toWrite = NMEA0183SimPrePlanned.pendingOutput.pop(0)
            if len(toWrite) > 0:
                toWrite = toWrite.rstrip() + '\r\n'
            self.SimWriteToSerial(toWrite)

        # Manage the time index - reset at end of preplanned path
        self._refSecs += 1
        if self._refSecs > PATHLEN:
            self._refSecs = 0
        log.debug('        GPS SIM INDEX: %d of %d' %(self._refSecs, PATHLEN))

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
        for sen in NMEA0183SimPrePlanned.SentenceStatus:
            if sen['Status'] == sim_NMEA0183.ON:
                if sen['Type'] == 'GPGGA':
                    nmeaOut = sim_NMEA0183.BuildGPGGA(n)
                    self.SimWriteToSerial(nmeaOut)
                elif sen['Type'] == 'GPRMC':
                    nmeaOut = sim_NMEA0183.BuildGPRMC(n)
                    self.SimWriteToSerial(nmeaOut)
                elif sen['Type'] == 'OOIXX':
                    nmeaOut = sim_NMEA0183.BuildOOIXX(n)
                    self.SimWriteToSerial(nmeaOut)

    def SimWriteToSerial(self, toWrite):
        """
        """
        if not self.IsSimOK():
            self._workingSim = False
            log.info('NMEA not written: no working simulator')
            return
        if not self.IsSocatRunning():
            self._goodComms = False
            log.info('NMEA not written: socat not running')
            return
        self.s.write(toWrite)
        log.debug('NMEA written: %s' % toWrite.rstrip())

    @defer.inlineCallbacks
    def SimShutdown(self):
        """
        Stop the GPS simulator from firing every second.
        """

        self._simRun.stop()
        log.info('1HZ GPS output has stopped.')
        yield pu.asleep(1)
        self._workingSim = False

    def IsSimulatorRunning(self):
        return self._workingSim

"""
Good PGRMC, all values valid:           $PGRMC,A,0.0,100,,,,,,A,5,0,1,0,1
Bad PGRMC, missing last value:          $PGRMC,A,0.0,100,,,,,,A,5,0,1,0
Bad PGRMC, invalid FIX_MODE:            $PGRMC,X,0.0,100,,,,,,A,5,0,1,0,1
Good PGRMC, just queries status:        $PGRMC
"""
#

if __name__ == '__main__':
    # Running as standalone app to spew data on a port
    log.info("Sim_NMEA0183_preplanned running as a standalone app!")
    
    # run our code
    simulator = NMEA0183SimPrePlanned()
    simulator.SetupSimulator()    

    # start the reactor
    reactor.run()
