#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/simulators/sim_NMEA0183.py
@brief Base class for GPS simulators that output NMEA0183 GPS to a local virtual serial port.
@author Alon Yaari
"""

from time import sleep
import os
import signal
from twisted.internet import defer, reactor
from datetime import datetime
import subprocess
from ion.util.procutils import asleep

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


SERPORTMASTER = '/tmp/serPortMaster'
SERPORTSLAVE = '/tmp/serPortSlave'
SOCATapp = 'socat'
SERPORTMODE = 'w+'
NULLPORTMODE = 'w'

# Open a null stream to pipe unwanted console messages to nowhere
nullDesc = open (os.devnull, NULLPORTMODE)

def DecDegToNMEAStr (dd):
    """
    Converts standard decimal degrees into NMEA's weird degree representation
    """

    # d = degrees (right side of decimal point is fraction of degree)
    # m = minutes (right side of decimal point is fraction of minute)
    # Input:  ddd.ddddddddd  (ex: 34.12341015)
    # Output: dddmm.mmmmmmm  (ex: 3407.404609)

    deg = int (dd)
    min = 60 * (dd - deg)
    return '%.6f' % (min + 100 * deg)

def DateTimeToDDMMYY (dt):
    """
    Converts a python datetime into DDMMYY
    """
    m = dt.month
    d = dt.day
    y = dt.year
    if m > 12 or d > 31 or y > 2100:
        return '000000'
    return '%02u%02u%02u' % (d, m, y)

def DateTimeToHHMMSS (dt):
    """
    Converts a python datetime into HHMMSS.
    """
    h = dt.hour
    m = dt.minute
    s = dt.second
    if h > 24 or m > 60 or s > 60:
        return '000000'
    return '%02u%02u%02u' % (h, m, s)

def CalcMagVar (lat, lon):
    if lat > 90.0 or lat < -90.0 or lon > 180.0 or lon < -180.0:
        return '999.9', 'X'
    # TODO: Properly Calculate the magnetic variation
    return '001.2', 'E'

def CalcChecksum (str):
    if len (str) < 1:
        return '00'
    # Calculate checksum
    # checksum = 8-bit XOR of all chars in string
    # checksum is an 8-bit number (0 to 255)
    # result is a hex byte version of the checksum
    cs = ord (reduce (lambda x, y: chr (ord (x) ^ ord (y)), str))
    calcHigh = 48 + ((cs & 240) >> 4)  # CS and 11110000
    calcLow = 48 + (cs & 15)           # CS and 00001111
    if calcHigh > 57:
        calcHigh += 7
    if calcLow > 57:
        calcLow += 7
    return chr(calcHigh) + chr(calcLow)

def BuildGPGGA (n):
    """
    Assemble an outgoing GPGGA sentence from the given dict info.
    """

    errFlag = False
    
    # 0     Sentence header
    str = ['GPGGA']

    # 1     UTC time of position fix in format hhmmss
    try:
        dt = n['time']
    except:
        errFlag = True
        dt = datetime.min
    str.append (DateTimeToHHMMSS (dt))

    # 2     Latitude as dd.mmmmmm
    try:
        lat = n['lat']
    except:
        errFlag = True
        lat = 0.0
    str.append (DecDegToNMEAStr (lat))

    # 3     Latitude hemisphere (pos lat = N, neg lat = S)
    str.append ('S' if lat < 0 else 'N')

    # 4     Longitude as ddd.mmmmmm
    try:
        lon = n['lon']
    except:
        errFlag = True
        lon = 0.0
    str.append (DecDegToNMEAStr (lon))

    # 5     Longitude hemisphere (pos lat = E, neg lat = W)
    str.append ('W' if lon < 0 else 'E')

    # 6     GPS quality info (0=nofix, 1=nondiff, 2=diff, 6=est)
    if errFlag:
        str.append ('0')
    else:
        str.append ('2')

    # 7     Number of satellites (00 to 12, incl. leading zero)
    if errFlag:
        str.append ('00')
    else:
        str.append ('%02u' % (5 + dt.minute % 4))

    # 8     HDOP (0.5 to 99.9)
    if errFlag:
        str.append ('99.9')
    else:
        str.append ('%.1f' %
                    ((3.9 + dt.minute % 5.0) + ((dt.second % 17.0) / 100.0)))

    # 9     Altitude above MSL (-9999.9 to 99999.9)
    altMSL = 3.2 + (dt.second % 30.0) / 100.0
    str.append ('%.1f' % altMSL)

    # 10    Altitude units (always 'M')
    str.append ('M' if altMSL else '')

    # 11    Altitude above geoid (-999.9 to 9999.9)
    altGeo = 78.8 + (dt.second % 30.0) / 100.0
    str.append ('%.1f' % altGeo)

    # 12    Altitude units (always 'M')
    str.append ('M' if altGeo else '')

    # 13    Not used
    str.append ('')

    # 14    Not used
    str.append ('')

    coreStr = ','.join (str)
    cs = CalcChecksum (coreStr)

    return '$' + coreStr + '*' + cs + '\r\n'


def BuildGPRMC (n):
    """
    Assemble an outgoing GPRMC sentence from the given dict info.
    """

    errFlag = False

    # 0     Sentence header
    str = ['GPRMC']

    # 1     UTC time of position fix in format hhmmss
    try:
        dt = n['time']
    except:
        errFlag = True
        dt = datetime.min
    str.append (DateTimeToHHMMSS (dt))

    # 2     Status (A = valid position, V = NAV receiver warning
    str.append ('A')

    # 3     Latitude as dd.mmmmmm
    try:
        lat = n['lat']
    except:
        errFlag = True
        lat = 0.0
    str.append (DecDegToNMEAStr (lat))

    # 4     Latitude hemisphere (pos lat = N, neg lat = S)
    str.append ('S' if lat < 0 else 'N')

    # 5     Longitude as ddd.mmmmmm
    try:
        lon = n['lon']
    except:
        errFlag = True
        lon = 0.0
    str.append (DecDegToNMEAStr (lon))

    # 6     Longitude hemisphere (pos lat = E, neg lat = W)
    str.append ('W' if lon < 0 else 'E')

    # 7     Speed Over Ground (000.0 to 999.9 knots with leading zeros)
    try:
        sogStr = n['sog']
    except:
        sogStr = '999.9'
    str.append (sogStr)

    # 8     Course Over Ground (000.0 to 359.9 degrees with leading zeros)
    try:
        cogStr = n['cog']
    except:
        cogStr = '999.9'
    str.append (cogStr)

    # 9     UTC date of position fix, ddmmyy
    try:
        dt = n['time']
    except:
        errFlag = True
        dt = datetime.min
    str.append (DateTimeToDDMMYY (dt))

    # 10    Magnetic variation (000.0 to 180.0 degrees with leading zeros)
    # 11    Magnetic variation direction (E or W west adds to true course)
    if errFlag:
        str.append ('999.9')
        str.append ('X')
    else:
        var, varDir = CalcMagVar (lat, lon)
        str.append (var)
        str.append (varDir)

    # 12    Mode indicator (A=Autonomous, D=Differental, E=Estimated, N=invalid)
    str.append ('A')

    coreStr = ','.join (str)
    cs = CalcChecksum (coreStr)

    return '$' + coreStr + '*' + cs + '\r\n'


class NMEA0183SimBase:
    """
    Parent class for NMEA0183 GPS simulators
    """

    #    Simulator connects to SERPORTMASTER
    #    driver_ NMEA0183 connects to SERPORTSLAVE

    WHICHSIM = 'NMEA0183 GPS Simulator Base Class'

    def __init__ (self):
        """
        Initializes the GPS Simulator
            - Generally best to not override this method
            - Calls SimGPSSetup() which should have an override
        """

        log.info ('simBase __init__')
        self._goodComms = False
        self._workingSim = False
        log.info ('----- Configuring the serial ports')
        self.SerialPortSetup()       # Sets up the serial port
        if not self._goodComms:
            log.error ('----- Serial ports not configured.')
            return
        log.info ('----- Serial ports configured.')
        self.SimGPSSetup()           # Inits the local simulator or launches external

    @defer.inlineCallbacks
    def SerialPortSetup (self):
        """
        Creates virtual serial ports then Launches the NEMA0183 GPS simulator
        @param None
        @retval True if successfully launched, False if not
        """
        log.info ('Starting serial port setup')
        self._goodComms = False

        # Create the virtual serial ports
        master = 'pty,link=' + SERPORTMASTER + ',raw,echo=0'
        slave = 'pty,link=' + SERPORTSLAVE + ',raw,echo=0'
        try:
            log.info ('----- Creating virtual serial port. Running %s...' % SOCATapp)
            self._vsp = subprocess.Popen([SOCATapp, master, slave],
                                        stdout = nullDesc.fileno(),
                                        stderr = nullDesc.fileno())
        except OSError, e:
            log.error ('----- Failure:  Could not create virtual serial port(s): %s' % e)
            return
        log.info ('----- Before sleep 5')
        sleep (1)
        log.info ('----- After sleep 5')
        #time.sleep (1)
        if not os.path.exists (SERPORTMASTER) and os.path.exists (SERPORTSLAVE):
            log.error ('Failure:  Unknown reason.')
            return
        log.info ('----- Successfully created virtual serial ports. socat PID: %d'
            % self._vsp.pid)
        self._serMaster = os.readlink (SERPORTMASTER)
        self._serSlave = os.readlink (SERPORTSLAVE)
        log.info ('----- Master port: %s   Slave port: %s' % (self._serMaster, self._serSlave))
        self._goodComms = True

    def SimGPSSetup(self):
        """
        Inits the local simulator or launches external simulator
        Expects override in child class
        """
        self._workingSim = False

    def StopSimulator(self):
        """
        Stops the simulator and disconnects serial ports
        - Generally best not to override this method
        - Calls SimShutdown() which should have an override
        @param None
        @retval None
        """

        self.SimShutdown()               # Stop the simulator
        log.info ('----- Freeing the serial ports...')

        # If the process isn't running any more, nothing to stop
        if  self.IsSocatRunning():

            # Force the socat app to stop
            # (Python 2.6 and later would let us send a control-C to stop it)
            os.kill (self._vsp.pid, signal.SIG_IGN)
        log.info ('----- Socat no longer running; serial ports freed.')

    def SimShutdown(self):
        """
        Shuts down the local simulator or stops external simulator
        Expects override in the child class
        """
        self._workingSim = False

    def IsSocatRunning (self):
        """
        Checks if the socat app is still running.
        @param None
        @retval True if running, False if not running
        """

        if self._goodComms:
            pollStatus = self._vsp.poll()
            if not pollStatus:  # poll() returns None if the app is running ok
                return True
        return False

    def IsSimulatorRunning (self):
        """
        Returns status of simulator.
            - Override in child simulator class
        """
        return False

    def IsSimOK (self):
        self._goodComms = self.IsSocatRunning()
        self._workingSim = self.IsSimulatorRunning()
        return self._workingSim


#



