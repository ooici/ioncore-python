#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/simulators/sim_NMEA0183.py
@brief Base class for GPS simulators that output NMEA0183 GPS to a local virtual serial port.
@author Alon Yaari
"""
import os
import signal
from twisted.internet import defer
from datetime import datetime
import subprocess
import tempfile
import ion.util.procutils as pu

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

tmpDir = tempfile.gettempdir()
SERPORTMASTER = tmpDir +  '/serPortMaster'
SERPORTSLAVE = tmpDir + '/serPortSlave'
SOCATapp = 'socat'
SERPORTMODE = 'w+'
NULLPORTMODE = 'w'
OFF = 'Off'
ON = 'On'

# Open a null stream to pipe unwanted console messages to nowhere
nullDesc = open(os.devnull, NULLPORTMODE)

def DecDegToNMEAStr(dd):
    """
    Converts standard decimal degrees into NMEA's weird degree representation
    """

    # d = degrees(right side of decimal point is fraction of degree)
    # m = minutes(right side of decimal point is fraction of minute)
    # Input:  ddd.ddddddddd (ex: 34.12341015)
    # Output: dddmm.mmmmmmm (ex: 3407.404609)

    deg = int(dd)
    min = 60 *(dd - deg)
    return '%.6f' %(min + 100 * deg)

def DateTimeToDDMMYY(dt):
    """
    Converts a python datetime into DDMMYY
    """
    m = dt.month
    d = dt.day
    y = dt.year
    if m > 12 or d > 31 or y > 2100:
        return '000000'
    return '%02u%02u%02u' %(d, m, y)

def DateTimeToHHMMSS(dt):
    """
    Converts a python datetime into HHMMSS.
    """
    h = dt.hour
    m = dt.minute
    s = dt.second
    if h > 24 or m > 60 or s > 60:
        return '000000'
    return '%02u%02u%02u' %(h, m, s)

def CalcMagVar(lat, lon):
    if lat > 90.0 or lat < -90.0 or lon > 180.0 or lon < -180.0:
        return '999.9', 'X'
    # TODO: Properly Calculate the magnetic variation
    return '001.2', 'E'

def CalcChecksum(str):
    if len(str) < 1:
        return '00'
    # Calculate checksum
    # checksum = 8-bit XOR of all chars in string
    # checksum is an 8-bit number(0 to 255)
    # result is a hex byte version of the checksum
    cs = ord(reduce(lambda x, y: chr(ord(x) ^ ord(y)), str))
    calcHigh = 48 +((cs & 240) >> 4)  # CS and 11110000
    calcLow = 48 +(cs & 15)           # CS and 00001111
    if calcHigh > 57:
        calcHigh += 7
    if calcLow > 57:
        calcLow += 7
    return chr(calcHigh) + chr(calcLow)

def BuildPGRMC():
    """
    Assemble an outgoing GPGGA sentence from stored configuration values.
    """
    # 0     Sentence header
    str = ['PGRMC']

    # 1     Fix Mode
    if NMEA0183SimBase.cfg_FIXMODE[0]:
        str.append(NMEA0183SimBase.cfg_FIXMODE[0])
    else:
        str.append('')

    # 2     Alt above/below MSL
    if NMEA0183SimBase.cfg_ALT:
        fVal = float(NMEA0183SimBase.cfg_ALT)
    else:
        fVal = float(0.0)
    str.append('%.1f' % fVal)

    # 3     Earth Datum Index
    if NMEA0183SimBase.cfg_DATUMINDEX:
        iVal = int(NMEA0183SimBase.cfg_DATUMINDEX)
    else:
        iVal = int(0)
    str.append('%d' % iVal)

    # 4     User earth datum
    str.append('0')

    # 5     User earth datum inverse flattening factor
    str.append('0')

    # 6     User earth datum delta X
    str.append('0')

    # 7     User earth datum delta Y
    str.append('0')

    # 8     User earth datum delta Z
    str.append('0')

    # 9     Differential mode
    if NMEA0183SimBase.cfg_DIFFMODE[0]:
        str.append(NMEA0183SimBase.cfg_DIFFMODE[0])
    else:
        str.append('')

    # 10     NMEA 0183 Baud rate
    if NMEA0183SimBase.cfg_BAUD:
        iVal = int(NMEA0183SimBase.cfg_BAUD)
    else:
        iVal = int(0)
    str.append('%d' % iVal)

    # 11     Velocity Filter
    if NMEA0183SimBase.cfg_VELFILTER:
        iVal = int(NMEA0183SimBase.cfg_VELFILTER)
    else:
        iVal = int(0)
    str.append('%d' % iVal)

    # 12     Measurement Pulse Output
    if NMEA0183SimBase.cfg_MPO:
        iVal = int(NMEA0183SimBase.cfg_MPO)
    else:
        iVal = int(0)
    log.debug('iVal type: %s' % type(iVal))
    str.append('%d' % iVal)

    # 13     Measurement Pulse Output Length
    if NMEA0183SimBase.cfg_MPOLEN:
        iVal = int(NMEA0183SimBase.cfg_MPOLEN)
    else:
        iVal = int(0)
    str.append('%d' % iVal)

    coreStr = ','.join(str)
    cs = CalcChecksum(coreStr)

    # 14    Dead reckoning valid time
    if NMEA0183SimBase.cfg_DEDRECKON:
        fVal = float(NMEA0183SimBase.cfg_DEDRECKON)
    else:
        fVal = float(0.0)
    str.append('%.1f' % fVal)

    coreStr = ','.join(str)
    return '$' + coreStr + '*' + cs + '\r\n'



def BuildGPGGA(n):
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
    str.append(DateTimeToHHMMSS(dt))

    # 2     Latitude as dd.mmmmmm
    try:
        lat = n['lat']
    except:
        errFlag = True
        lat = 0.0
    str.append(DecDegToNMEAStr(abs(lat)))

    # 3     Latitude hemisphere(pos lat = N, neg lat = S)
    str.append('S' if lat < 0 else 'N')

    # 4     Longitude as ddd.mmmmmm
    try:
        lon = n['lon']
    except:
        errFlag = True
        lon = 0.0
    str.append(DecDegToNMEAStr(abs(lon)))

    # 5     Longitude hemisphere(pos lat = E, neg lat = W)
    str.append('W' if lon < 0 else 'E')

    # 6     GPS quality info(0=nofix, 1=nondiff, 2=diff, 6=est)
    if errFlag:
        str.append('0')
    else:
        str.append('2')

    # 7     Number of satellites(00 to 12, incl. leading zero)
    if errFlag:
        str.append('00')
    else:
        str.append('%02u' %(5 + dt.minute % 4))

    # 8     HDOP(0.5 to 99.9)
    if errFlag:
        str.append('99.9')
    else:
        str.append('%.1f' %
                   ((3.9 + dt.minute % 5.0) +((dt.second % 17.0) / 100.0)))

    # 9     Altitude above MSL(-9999.9 to 99999.9)
    altMSL = 3.2 +(dt.second % 30.0) / 100.0
    str.append('%.1f' % altMSL)

    # 10    Altitude units(always 'M')
    str.append('M' if altMSL else '')

    # 11    Altitude above geoid(-999.9 to 9999.9)
    altGeo = 78.8 +(dt.second % 30.0) / 100.0
    str.append('%.1f' % altGeo)

    # 12    Altitude units(always 'M')
    str.append('M' if altGeo else '')

    # 13    Not used
    str.append('')

    # 14    Not used
    str.append('')

    coreStr = ','.join(str)
    cs = CalcChecksum(coreStr)

    return '$' + coreStr + '*' + cs + '\r\n'

def BuildGPRMC(n):
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
    str.append(DateTimeToHHMMSS(dt))

    # 2     Status(A = valid position, V = NAV receiver warning
    str.append('A')

    # 3     Latitude as dd.mmmmmm
    try:
        lat = n['lat']
    except:
        errFlag = True
        lat = 0.0
    str.append(DecDegToNMEAStr(abs(lat)))

    # 4     Latitude hemisphere(pos lat = N, neg lat = S)
    str.append('S' if lat < 0 else 'N')

    # 5     Longitude as ddd.mmmmmm
    try:
        lon = n['lon']
    except:
        errFlag = True
        lon = 0.0
    str.append(DecDegToNMEAStr(abs(lon)))

    # 6     Longitude hemisphere(pos lat = E, neg lat = W)
    str.append('W' if lon < 0 else 'E')

    # 7     Speed Over Ground(000.0 to 999.9 knots with leading zeros)
    try:
        sogStr = n['sog']
    except:
        sogStr = '999.9'
    str.append(sogStr)

    # 8     Course Over Ground(000.0 to 359.9 degrees with leading zeros)
    try:
        cogStr = n['cog']
    except:
        cogStr = '999.9'
    str.append(cogStr)

    # 9     UTC date of position fix, ddmmyy
    try:
        dt = n['time']
    except:
        errFlag = True
        dt = datetime.min
    str.append(DateTimeToDDMMYY(dt))

    # 10    Magnetic variation(000.0 to 180.0 degrees with leading zeros)
    # 11    Magnetic variation direction(E or W west adds to true course)
    if errFlag:
        str.append('999.9')
        str.append('X')
    else:
        var, varDir = CalcMagVar(lat, lon)
        str.append(var)
        str.append(varDir)

    # 12    Mode indicator(A=Autonomous, D=Differental, E=Estimated, N=invalid)
    str.append('A')

    coreStr = ','.join(str)
    cs = CalcChecksum(coreStr)

    return '$' + coreStr + '*' + cs + '\r\n'

def BuildOOIXX(n):
    """
    Assemble an outgoing OOIXX sentence from the given dict info.
    """

    errFlag = False
    
    # 0     Sentence header
    str = ['OOIXX']

    # 1     UTC time of position fix in format hhmmss
    try:
        dt = n['time']
    except:
        errFlag = True
        dt = datetime.min
    str.append(DateTimeToHHMMSS(dt))

    # 2     Latitude as dd.mmmmmm
    try:
        lat = n['lat']
    except:
        errFlag = True
        lat = 0.0
    str.append(DecDegToNMEAStr(abs(lat)))

    # 3     Latitude hemisphere(pos lat = N, neg lat = S)
    str.append('S' if lat < 0 else 'N')

    # 4     Longitude as ddd.mmmmmm
    try:
        lon = n['lon']
    except:
        errFlag = True
        lon = 0.0
    str.append(DecDegToNMEAStr(abs(lon)))

    # 5     Longitude hemisphere(pos lat = E, neg lat = W)
    str.append('W' if lon < 0 else 'E')

    # 6     GPS quality info(0=nofix, 1=nondiff, 2=diff, 6=est)
    if errFlag:
        str.append('0')
    else:
        str.append('2')

    # 7     Number of satellites(00 to 12, incl. leading zero)
    if errFlag:
        str.append('00')
    else:
        str.append('%02u' %(5 + dt.minute % 4))

    # 8     HDOP(0.5 to 99.9)
    if errFlag:
        str.append('99.9')
    else:
        str.append('%.1f' %
                   ((3.9 + dt.minute % 5.0) +((dt.second % 17.0) / 100.0)))

    # 9     Altitude above MSL(-9999.9 to 99999.9)
    altMSL = 3.2 +(dt.second % 30.0) / 100.0
    str.append('%.1f' % altMSL)

    # 10    Altitude units(always 'M')
    str.append('M' if altMSL else '')

    # 11    Course over ground COG (HEADING) in degrees from true north (0.0 to 359.9)
    str.append(n['cog'])

    # 12    Speed over ground SOG (SPEEDMS) in meters per second (0.0 to 999.9)
    str.append(n['sog'])

    # 13    Not used
    str.append('')

    # 14    Not used
    str.append('')

    coreStr = ','.join(str)
    cs = CalcChecksum(coreStr)

    return '$' + coreStr + '*' + cs + '\r\n'

class NMEA0183SimBase:
    """
    Parent class for NMEA0183 GPS simulators
    """

    #    Simulator connects to SERPORTMASTER
    #    driver_ NMEA0183 connects to SERPORTSLAVE

    WHICHSIM = 'NMEA0183 GPS Simulator Base Class'

    # PGRMC CONFIGURATION SETTINGS

    # <1> FIX MODE
    #       A = automatic
    #       2 = 2D(host must supply alt)
    #       3 = 3D exclusively
    cfg_FIXMODE = 'A'

    # <2> ALT MEAN SEA LEVEL
    #       -1500.0 to 18000.0 meters
    cfg_ALT = 0.0

    # <3> EARTH DATUM INDEX
    #       0 to 109(IDs of valid earth datum indices)
    #       96 IS NOT ALLOWED, it is user specified and not supported here
    #       100 is WGS84
    #       NOTE        Setting to any value except 96 will work but GPS
    #                   output will always be in WGS84
    cfg_DATUMINDEX = 100

    # <4> USER EARTH DATUM SEMI-MAJOR AXIS
    #       Always set to 0

    # <5> USER EARTH DATUM INVERSE FLATTENING FACTOR
    #       Always set to 0

    # <6> USER EARTH DATUM DELTA X
    #       Always set to 0

    # <7> USER EARTH DATUM DELTA Y
    #       Always set to 0

    # <8> USER EARTH DATUM DELTA Z
    #       Always set to 0

    # <9> DIFFERENTIAL MODE
    #       A = automatic(DGPS when available)
    #       D = differential exclusively(no output if not diff fix)
    #       NOTE        Setting will store but will make no difference
    #                   in GPS output.
    cfg_DIFFMODE = 'A'

    # <10> NMEA 0183 BAUD RATE
    #       1 = 1200
    #       2 = 2400
    #       3 = 4800
    #       4 = 9600
    #       5 = 19200
    #       6 = 300
    #       7 = 600
    #       8 = 38400
    cfg_BAUD = 5

    # <11> VELOCITY FILTER
    #       0 = No Filter
    #       1 = Automatic filter
    #       2-255 = Filter time constant, in seconds
    #       NOTE        Setting will store but will make no difference
    #                   in GPS output.
    cfg_VELFILTER = 0

    # <12> MEASUREMENT PULSE OUTPUT
    #       1 = Disabled
    #       2 = Enabled
    #       NOTE        Setting will store but will make no difference
    #                   in GPS output.
    cfg_MPO = 0

    # <13> MEASUREMENT PULSE OUTPUT LENGTH
    #       0 to 48
    #       NOTE        Setting will store but will make no difference
    #                   in GPS output.
    cfg_MPOLEN = 0

    # <14> DEAD RECKONING VALID TIME
    #       1 to 30
    #       NOTE        Setting will store but will make no difference
    #                   in GPS output.
    cfg_DEDRECKON = 0

    def __init__(self):
        """
        Initializes the GPS Simulator
            - Generally best to not override this method
            - Calls SimGPSSetup() which should have an override
        """

        log.info('simBase __init__')
        self._goodComms = False
        self._workingSim = False
        
    @defer.inlineCallbacks
    def SetupSimulator(self):
        yield self.SerialPortSetup()       # Sets up the serial port
        if not self._goodComms:
            log.error('Serial ports not configured.')
            return
        log.info('Serial ports configured.')
        yield self.SimGPSSetup()           # Inits the selected simulator

    @defer.inlineCallbacks
    def SerialPortSetup(self):
        """
        Creates virtual serial ports then Launches the NEMA0183 GPS simulator
        @param None
        @retval True if successfully launched, False if not
        """
        self._goodComms = False

        # Create the virtual serial ports
        master = 'pty,link=' + SERPORTMASTER + ',raw,echo=0'
        slave = 'pty,link=' + SERPORTSLAVE + ',raw,echo=0'
        try:
            log.info('Creating virtual serial port. Running %s...' % SOCATapp)
            self._vsp = subprocess.Popen([SOCATapp, master, slave],
                                         stdout = nullDesc.fileno(),
                                         stderr = nullDesc.fileno())
        except OSError, e:
            log.error('Failure:  Could not create virtual serial port(s): %s' % e)
            return
        yield pu.asleep(1) # wait just a bit for connect
        if not os.path.exists(SERPORTMASTER) and os.path.exists(SERPORTSLAVE):
            log.error('Failure:  Unknown reason.')
            return
        log.debug('Successfully created virtual serial ports. socat PID: %d'
            % self._vsp.pid)
        self._serMaster = os.readlink(SERPORTMASTER)
        self._serSlave = os.readlink(SERPORTSLAVE)
        log.debug('Master port: %s   Slave port: %s' %(self._serMaster, self._serSlave))
        self._goodComms = True

    @defer.inlineCallbacks
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
        log.debug('Freeing the serial ports...')

        # If the process isn't running any more, nothing to stop
        if  self.IsSocatRunning():

            # Force the socat app to stop
            #(Python 2.6 and later would let us send a control-C to stop it)
            os.kill(self._vsp.pid, signal.SIG_IGN)
        log.debug('Socat no longer running; serial ports freed.')

    def SimShutdown(self):
        """
        Shuts down the local simulator or stops external simulator
        Expects override in the child class
        """
        self._workingSim = False

    def IsSocatRunning(self):
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

    def IsSimulatorRunning(self):
        """
        Returns status of simulator.
            - Override in child simulator class
        """
        return False

    def IsSimOK(self):
        self._goodComms = self.IsSocatRunning()
        self._workingSim = self.IsSimulatorRunning()
        return self._workingSim