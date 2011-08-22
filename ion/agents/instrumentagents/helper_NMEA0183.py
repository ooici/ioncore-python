#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/helper_NMEA0183.py
@author Alon Yaari
@brief Helper code for working with NMEA0183 devices and parsing NMEA0183 strings
"""

from string import hexdigits
from ion.agents.instrumentagents.instrument_constants import InstErrorCode, BaseEnum
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# Constants
CR = '\r'
LF = '\n'
CRLF = '\r\n'
MIN_NMEA_LEN = 6
MAX_NMEA_LEN = 82
gpsNAN = -999.9

class NMEAErrorCode (InstErrorCode):
    """
    Additional error codes unique to NMEA strings.
    """
    OK = ['OK']
    INVALID_NMEA_STRING = ['INVALID_NMEA','NMEA String improperly configured.']
    INVALID_CHECKSUM = ['INVALID_CHECKSUM', 'Checksums do not agree.']
    UNKNOWN_NMEA_CODE = ['UNKNOWN_NMEA','5-character NMEA code is not defined.']
    INVALID_DATA_ITEMS = ['NO_NMEA_DATA','Exepected more data items']
    INTERNAL_ERROR = ['INTERNAL_ERROR','Internal code error.']

class NMEADefs (BaseEnum):
    """
    Instructions to parse known NMEA strings.
    """

    newsOffset = {'N': 1, 'E': 1, 'W': -1, 'S': -1}
    fixQuality = ({0: 'Invalid'},
                  {1: 'GPS Fix (SPS)'},
                  {2: 'DGPS Fix'},
                  {3: 'PPS Fix'},
                  {4: 'Real Time Kinematic'},
                  {5: 'Float RTK'},
                  {6: 'Estimated (dead reckon)'},
                  {7: 'Manual Input Mode'},
                  {8: 'Simulation Mode'})

    dataActive = {'A': 'Autonomous',
                  'D': 'Differential',
                  'E': 'Estimated',
                  'N': 'Not Valid',
                  'S': 'Simulated'}

    # NMEA_CD  string  5-character NMEA data type code
    # DESC     string  Short description of the NMEA sentence
    # UTC_HMS  int ot float UTC time on a 24hr clock as HHMMSS.S (1hz GPS as HHMMSS)ex: 123519.2  = 12:35:19.2
    # UTC_DMY  int     6-digit date as ddmmyy                                  ex: 250411
    # RAW_LAT  float   Lat as ddmm.mmm, needs converting to decimal degrees    ex: 4807.038  = 48deg 7.038min
    # LAT_DIR  char    N or S (if S, lat is negative)
    # RAW_LON  float   Lon as dddmm.sss, needs converting to decimal degrees   ex: 01131.051 = 11deg 31.051min
    # LON_DIR  char    E or W (if W, lon is negative)
    # FIX_QUA  int     See fixQuality list above                               ex: 2         = DGPS Fix
    # NUM_SAT  int     Number of tracked satellites (<10 may have  leading 0)  ex: 08        = 8 satellites
    # HOR_DOP  float   Relative accuracy of horizontal position (in m)         ex: 0.9       = .9 meters HDOP
    # ALT_MSL  float   Altitude above mean sea level (MSL)                     ex: 545.4     = 545.4 alt MSL
    # MSLUNIT  char    Units for altitude mean sea level, M = meters
    # ALT_GEO  float   Altitude above mean geoid (usually WGS84?)              ex: 342.7     = 342.7 alt MSL
    # GEOUNIT  char    Units for altitude geoid, M = meters
    # DATA_AC  char    See dataActive list above                               ex: A
    # STATUS   char    A=Valid position V=NVA error                            ex: A
    # SPD_GND  float   Speed over ground in knots                              ex: 22.4
    # TRK_DEG  float   Track angle in degrees relative to true north           ex: 95.3
    # RAW_MAG  float   Magnetic variation in degrees                           ex: 003.3
    # MAG_DIR  char    Direction of magnetic variation E=east W=west           ex: E
    # WEEK_NO  int     GPS week number (0 to 1023)
    # SECONDS  int     GPS seconds (0 to 604799)
    # LEAPSEC  int     GPS leap second count

    nmeaInTypes = { \
    'PGRMO': ['Output Sentence Enable/Disable', #0
              'TARGET',                         #1
              'PGRMODE'],                       #2

    'PGRMC': ['Garmin Sensor Configuration Information', # 0
              'FIX_MODE',        # 1
              'ALT_MSL',         # 2
              'E_DATUM',         # 3
              'SM_AXIS',         # 4
              'DATUMIFF',        # 5
              'DATUM_DX',        # 6
              'DATUM_DY',        # 7
              'DATUM_DZ',        # 8
              'DIFFMODE',        # 9
              'BAUD_RT',         # 10
              'VEL_FILT',         # 11
              'MP_OUT',          # 12
              'MP_LEN',          # 13
              'DED_REC']}        # 14

    nmeaTypes = { \
    'GPGGA': {'Parsing': ['GPS Fix Data',    #0
                          'UTC_HMS',         #1
                          'RAW_LAT',         #2
                          'LAT_DIR',         #3
                          'RAW_LON',         #4
                          'LON_DIR',         #5
                          'FIX_QUA',         #6
                          'NUM_SAT',         #7
                          'HOR_DOP',         #8
                          'ALT_MSL',         #9
                          'MSLUNIT',         #10
                          'ALT_GEO',         #11
                          'GEOUNIT'],        #12
               'Output': ['NMEA_CD',
                          'DESC',
                          'HOUR',
                          'MIN',
                          'SEC',
                          'MS',              # Only if GPS > 1hz
                          'GPS_LAT',
                          'GPS_LON',
                          'FIX_QUA',
                          'NUM_SAT',
                          'HDOP',
                          'ALT_MSL',
                          'MSLUNIT',
                          'ALT_GEO',
                          'GEOUNIT',
                          'DATA_AC']},
    'OOIXX': {'Parsing': ['OOI Custom Sentence',    #0
                          'UTC_HMS',         #1
                          'RAW_LAT',         #2
                          'LAT_DIR',         #3
                          'RAW_LON',         #4
                          'LON_DIR',         #5
                          'FIX_QUA',         #6
                          'NUM_SAT',         #7
                          'HOR_DOP',         #8
                          'ALT_MSL',         #9
                          'MSLUNIT',         #10
                          'COURSE',          #11
                          'SPD_MPS'],        #12
               'Output': ['NMEA_CD',
                          'DESC',
                          'HOUR',
                          'MIN',
                          'SEC',
                          'MS',              # Only if GPS > 1hz
                          'GPS_LAT',
                          'GPS_LON',
                          'FIX_QUA',
                          'NUM_SAT',
                          'HDOP',
                          'ALT_MSL',
                          'MSLUNIT',
                          'ALT_GEO',
                          'COURSE',
                          'SPD_MPS',
                          'SPD_KPH']},
    'XXXXX': {'Parsing': ['Dummy Heartbeat',  # 0
                          'IGNORE'],         # 1
               'Output': ['NMEA_CD']},
    'GPGLL': {'Parsing': ['GPS Latitude and Longitude',  # 0
                          'RAW_LAT',         # 1
                          'LAT_DIR',         # 2
                          'RAW_LON',         # 3
                          'LON_DIR',         # 4
                          'UTC_HMS',         # 5
                          'DATA_AC'],        # 6
               'Output': ['NMEA_CD',
                          'DESC',
                          'GPS_LAT',
                          'GPS_LON',
                          'HOUR',
                          'MIN',
                          'SEC',
                          'MS',              # Only if GPS > 1hz
                          'DATA_AC']},
    'GPRMC': {'Parsing': ['Recommended Minimum Senence C', # 0
                          'UTC_HMS',         # 1
                          'STATUS',          # 2
                          'RAW_LAT',         # 3
                          'LAT_DIR',         # 4
                          'RAW_LON',         # 5
                          'LON_DIR',         # 6
                          'SPD_KTS',         # 7
                          'TRK_DEG',         # 8
                          'DATE',            # 9
                          'RAW_MAG'          # 10
                          'MAG_DIR'],        # 11
               'Output': ['HOUR',
                          'MIN',
                          'SEC',
                          'MS',              # Only if GPS > 1hz
                          'STATUS',
                          'GPS_LAT',
                          'GPS_LON',
                          'SPD_KTS',
                          'SPD_MPS',
                          'TRK_DEG',
                          'DAY',
                          'MONTH',
                          'YEAR',
                          'MAG_VAR']},
    'PGRMC': {'Parsing': ['Garmin Sensor Configuration Information', # 0
                          'FIX_MODE',        # 1
                          'ALT_MSL',         # 2
                          'E_DATUM',         # 3
                          'SM_AXIS',         # 4
                          'DATUMIFF',        # 5
                          'DATUM_DX',        # 6
                          'DATUM_DY',        # 7
                          'DATUM_DZ',        # 8
                          'DIFFMODE',        # 9
                          'BAUD_RT',         # 10
                          'IGNORE',          # 11
                          'MP_OUT',          # 12
                          'MP_LEN',          # 13
                          'DED_REC'],        # 14
               'Output': ['FIX_MODE',
                          'ALT_MSL',
                          'E_DATUM',
                          'SM_AXIS',
                          'DATUMIFF',
                          'DATUM_DX',
                          'DATUM_DY'
                          'DATUM_DZ'
                          'DIFFMODE',
                          'BAUD_RT',
                          'MP_OUT',
                          'MP_LEN',
                          'DED_REC']},
    'PGRMF': {'Parsing': ['Garmin GPS Fix Data Sentence', # 0
                          'WEEK_NO',         # 1
                          'SECONDS',         # 2
                          'UTC_DMY',         # 3
                          'UTC_HMS',         # 4
                          'LEAPSEC',         # 5
                          'RAW_LAT',         # 6
                          'LAT_DIR',         # 7
                          'RAW_LON',         # 8
                          'LON_DIR',         # 9
                          'GPSMODE',         # 10
                          'FIXTYPE',         # 11
                          'SPD_KPH',         # 12
                          'COURSE',          # 13
                          'PDOP',            # 14
                          'TDOP'],           # 15
               'Output': ['DAY',
                          'MONTH',
                          'YEAR',
                          'HOUR',
                          'MIN',
                          'SEC',
                          'MS',              # Only if GPS > 1hz
                          'GPS_LAT',
                          'GPS_LON',
                          'GPSMODE',
                          'FIXTYPE',
                          'SPD_KTS',
                          'SPD_MPS',
                          'COURSE',
                          'PDOP',
                          'TDOP']}}

class NMEAInString ():
    """
    Representation of a single ASCII NMEA command (sent to the NMEA device).
    """

    def __init__ (self, inNMEA):
        """
        Takes the input NMEA string through the parsing process.
        """
        self._inNMEA = inNMEA
        self._valid = self.ValidateInNMEA()

    def ValidateInNMEA (self):
        """
        Checks that an NMEA string is valid.
        @retval OK if valid NMEA string, otherwise relevant error code
        """

        # Rules for a valid input NMEA string:
        #   - Must always start with '$'
        #   - Must always end with <CR>, <LF>, or <CR><LF>
        #   - Maximum 82 characters, inclusive of '$' and <CR><LF>
        #   - Must have 5 chars immediately after '$'
        #   - Therefore, minimum string is "$XXXXX<CR><LF>" len = 8

        # Validate NMEA string length
        nmeaLen = len (self._inNMEA)
        if nmeaLen < MIN_NMEA_LEN or nmeaLen > MAX_NMEA_LEN:
            return NMEAErrorCode.INVALID_NMEA_STRING

        # Validate '$'
        if self._inNMEA[0] != '$':
            return NMEAErrorCode.INVALID_NMEA_STRING

        # Strip off <CR>, <LF>, and <CR><LF>
        self._inNMEA.rstrip()

        # Verify that there is at least one data element to parse
        # 0 1 2
        #  $ X ,<end>    minimum possible NMEA string
        try:
            firstComma = self._inNMEA.index (',', 2)
        except ValueError:
            return NMEAErrorCode.INVALID_NMEA_STRING

        # Strip out NMEA type code and verify it is known to this parser
        self._nmeaType = self._inNMEA[1:firstComma]
        if not NMEADefs.nmeaInTypes.has_key (self._nmeaType):
            return NMEAErrorCode.UNKNOWN_NMEA_CODE

        return NMEAErrorCode.OK

    def IsValid (self):
        return NMEAErrorCode.is_error (self._valid)

    def GetNMEAInData (self):
        """
        If the NMEA string was valid, returns the parsed data.
        @retval             Dict of GPS data, otherwise error code
        """

        if NMEAErrorCode.is_error (self._valid):
            return self._valid
        parsedOK = self.ParseNMEA()
        if NMEAErrorCode.is_error (parsedOK):
            return parsedOK
        return self._dataOut

    def ParseNMEA (self):
        """
        Main parsing routine for an NMEA input string.
        @retval Dict of nmeaDefs and parsed values or error relevant code
        """

        self._dataOut = {}
        parsed = self._inNMEA.upper().split (',')
        nmeaType = parsed[0][1:]
        howToParse = NMEADefs.nmeaInTypes.get (nmeaType, 'NODT')
        if howToParse == 'NODT':
            return NMEAErrorCode.UNKNOWN_NMEA_CODE

        # Must have enough data elements to parse
        if len (parsed) < len (howToParse):
            return NMEAErrorCode.INVALID_DATA_ITEMS

        # NMEA_CD  string  5-character NMEA data type code
        self._dataOut['NMEA_CD'] = nmeaType

        # DESC     string  Short description of the NMEA sentence
        self._dataOut['DESC'] = howToParse[0]

        for howTo in howToParse[1:]:

            item = parsed[howToParse.index (howTo)]

            # TARGET  string   For $PFRMO, the sentence to turn on or off
            if howTo == 'TARGET':
                self._dataOut['TARGET'] = item

            # PGRMODE int      Target sentence mode
            if howTo == 'PGRMODE':
                self._dataOut['PGRMODE'] = int (item)

            # FIX_MODE char  Combine of FIX_TYPE and GPS_MODE
            #                A  = Automatic, 3 = 3D only
            if howTo == 'FIX_MODE':
                self._dataOut['FIX_MODE'] = item

            # E_DATUM  int  Earth datum ID number
            if howTo == 'E_DATUM':
                self._dataOut['E_DATUM'] = item

            # SM_AXIS   Relevant only if E_DATUM == USERDEF
            if howTo == 'SM_AXIS':
                pass

            # DATUMIFF   Relevant only if E_DATUM == USERDEF
            if howTo == 'DATUMIFF':
                pass

            # DATUM_DX   Relevant only if E_DATUM == USERDEF
            if howTo == 'DATUM_DX':
                pass

            # DATUM_DY   Relevant only if E_DATUM == USERDEF
            if howTo == 'DATUM_DY':
                pass

            # DATUM_DZ   Relevant only if E_DATUM == USERDEF
            if howTo == 'DATUM_DZ':
                pass

            # DIFFMODE  char  Differential mode
            if howTo == 'DIFFMODE':
                self._dataOut['DIFFMODE'] = item

            # BAUD_RT  int  NMEA 0183 Baud Rate
            if howTo == 'BAUD_RT':
                self._dataOut['BAUD_RT'] = item

            # MP_OUT  int  Measurement Pulse Output
            if howTo == 'MP_OUT':
                self._dataOut['MP_OUT'] = item

            # MP_LEN  int  Measurement Pulse Output pulse length ((n+1)* 20ms)
            if howTo == 'MP_LEN':
                self._dataOut['MP_LEN'] = item

            # DED_REC  float  Ded. Reckoning valid time 0.2 to 30.0 sec
            if howTo == 'DED_REC':
                self._dataOut['DED_REC'] = item

        return NMEAErrorCode.OK


class NMEAString ():
    """
    Representation of a single ASCII NMEA string (from the NMEA device).
    """

    def __init__(self, nmeaString):
        """
        Takes the NMEA string through the entire parsing process.
        @param  nmeaString  Complete NMEA line from $ to <CR><LF>
        """

        self.nmeaStr = nmeaString
        self.valid = self.ValidateNMEA()

    def IsValid (self):
        """
        Reports on the validity of the NMEA string.
        @retval             OK, otherwise relevant error code
        """
        return self.valid

    def GetNMEAData (self):
        """
        If the NMEA string was valid, returns the parsed data.
        @retval             Dict of GPS data, otherwise error code
        """

        if NMEAErrorCode.is_error (self.valid):
            return self.valid
        parsedOK = self.ParseNMEA()
        if NMEAErrorCode.is_error (parsedOK):
            return parsedOK
        return self.dataOut

    def NMEAChecksum (self, nmeaCS, checkH, checkL):
        """
        Calculates NMEA checksum and compares it with the in-string value.
        @param  nmeaCS Only the characters between '$' and '*' exclusive
        @param  checkH left (most sig.) hex nibble of the NMEA's checksum
        @param  checkL right (least sig.) hex nibble of the NMEA's checksum
        @retval        OK if checksums match, otherwise relevant error code
        """

        # Validate the hex nibbles
        if checkH not in hexdigits or checkL not in hexdigits:
            return NMEAErrorCode.INVALID_NMEA_STRING

        # Calculate checksum
        # checksum = 8-bit XOR of all chars in string
        # result is an 8-bit number (0 to 255)
        cs = ord (reduce (lambda x, y: chr (ord (x) ^ ord (y)), nmeaCS))
        low = 48 + (cs & 15)           # CS and 00001111
        if low > 57:
            low += 7;
        high = 48 + ((cs & 240) >> 4)  # CS and 11110000
        if high > 58:
            high += 7;
        calcLow = chr (low)
        calcHigh = chr (high)

        # Validate calculated against what the NMEA string said it should be
        if (calcLow == checkL and calcHigh == checkH):
            return NMEAErrorCode.OK
        return NMEAErrorCode.INVALID_NMEA_STRING

    def ValidateNMEA (self):
        """
        Checks that an NMEA string is valid.
        @retval OK if valid NMEA string, otherwise relevant error code
        """

        # Rules for a valid NMEA string:
        #   - Must always start with '$'
        #   - Must always end with <CR><LF>
        #   - Maximum 82 characters, inclusive of '$' and <CR><LF>
        #   - Must have 5 chars immediately after '$'
        #   - Therefore, minimum string is "$XXXXX<CR><LF>" len = 8
        #   - Optional checksum:
        #       - Indicated with a '*' after last data element, before <CR><LF>
        #       - two-printable chars between '*' and <CR><LF>
        #       - Checksum = 8-bit XOR of all chars between $ and * exclusive

        # Validate NMEA string length
        nmeaLen = len (self.nmeaStr)
        if nmeaLen < MIN_NMEA_LEN or nmeaLen > MAX_NMEA_LEN:
            return NMEAErrorCode.INVALID_NMEA_STRING

        # Validate '$'
        if self.nmeaStr[0] != '$':
            return NMEAErrorCode.INVALID_NMEA_STRING

        # Properly formed NMEA strings must end in <CR><LF>
        # Reality is that some devices don't do it or that middleware changes
        # it to some variant.  It is also convenient in testing to not have
        # them in place.
        # Therefore, this code does not enforce the presence of <CR><LF> and
        # in fact allows any combination of <CR>, <LF>, both, or none.
        self.nmeaStr.rstrip()

        # Verify that there is at least one data element to parse
        # 0 1 2
        #  $ X ,<end>    minimum possible NMEA string
        try:
            firstComma = self.nmeaStr.index(',', 2)
        except ValueError:
            return NMEAErrorCode.INVALID_NMEA_STRING

        # Strip out NMEA type code and verify it is known to this parser
        self.nmeaType = self.nmeaStr[1:firstComma]
        if not NMEADefs.nmeaTypes.has_key (self.nmeaType):
            return NMEAErrorCode.UNKNOWN_NMEA_CODE

        # Validate CEHCKSUM (if there is one)
        #       Since <CR><LF> was stripped off, '*' marking the
        #       checksum will always be at location [-3]
        # -3 -2 -1
        #   *  F  F<end>
        if self.nmeaStr[-3] == '*':
            self.nmeaData = self.nmeaStr[1: -3]  # Strip off checksum and '$'
            goodSum = self.NMEAChecksum (self.nmeaData,
                                         self.nmeaStr[-2],
                                         self.nmeaStr[-1])
            if NMEAErrorCode.is_error(goodSum):
                return NMEAErrorCode.INVALID_CHECKSUM

        else:
            self.nmeaData = self.nmeaStr[1:]     # Strip off checksum
        return NMEAErrorCode.OK

    def NMEAStrToFloat (self, inStr):
        """
        Converts string to float for NMEA string processing.
        @param  inStr       Input float as a string
        @retval             Float value or gpsNAN on not a float value error
        """

        try:
            return float (inStr)
        except:
            return gpsNAN

    def NMEADegMinToDecDeg (self, inFloat):
        """
        Converts DDMM.MMMM or DDDMM.MMMM into DD.DDDDDDDD.
        @param  inFloat     Input float value
        @retval             Float in decimal degrees
        """

        deg = int (inFloat / 100.0)
        min = inFloat - (deg * 100.0)
        return deg + min / 60.0

    def ParseNMEA (self):
        """
        Main parsing routine for an NMEA string.
        @retval Dict of nmeaDefs and parsed values or error relevant code
        """

        parsed = self.nmeaData.upper().split (',')
        nmeaType = parsed[0]
        howToParse = NMEADefs.nmeaTypes.get (nmeaType, 'NODT')
        if howToParse == 'NODT':
            return NMEAErrorCode.UNKNOWN_NMEA_CODE
        howToParse = howToParse['Parsing']
        if howToParse == 'NODT':
            return NMEAErrorCode.INTERNAL_ERROR


        # Must have enough data elements to parse
        if len (parsed) < len (howToParse):
            return NMEAErrorCode.INVALID_DATA_ITEMS

        # Break apart the data values and deal with each type
        nmeaType = parsed[0]
        dataOut = {}

        # NMEA_CD  string  5-character NMEA data type code
        dataOut['NMEA_CD'] = nmeaType

        # DESC     string  Short description of the NMEA sentence
        dataOut['DESC'] = howToParse[0]

        rawLat = gpsNAN
        rawLon = gpsNAN

        for howTo in howToParse[1:]:

            item = parsed[howToParse.index (howTo)]

            # UTC_HMS  double  UTC time on a 24hr clock as HHMMSS.S
            #                  (1hz GPS as HHMMSS)
            #                  ex: 123519.2  = 12:35:19.2
            if howTo == 'UTC_HMS':
                # Valid time is HHMMSS or HHMMSS.S
                decPos = item.find ('.')
                if (len (item) == 6) or (decPos == 6):
                    dataOut['HOUR'] = int (item[:2])
                    dataOut['MIN'] = int (item[2:4])
                    dataOut['SEC'] = int (item[4:6])
                    if decPos == 6:
                        dataOut['MS'] = int (float (item[6:]) * 1000.0)

            # RAW_LAT  double  Lat as ddmm.mmm, needs converting to dec. deg.
            #                  ex: 4807.038  = 48deg 7.038min
            if howTo == 'RAW_LAT':
                rawLat = self.NMEAStrToFloat (item)
                
            # LAT_DIR  char    N or S (if S, lat is negative)
            if howTo == 'LAT_DIR':
                offset = NMEADefs.newsOffset.get (item, gpsNAN)
                rawLat = self.NMEADegMinToDecDeg (rawLat) * offset
                if abs (rawLat) <= 90.0:
                    dataOut['GPS_LAT'] = rawLat

            # RAW_LON  double  Lon as dddmm.sss, needs converting to dec. deg.
            #                  ex: 01131.051 = 11deg 31.051min
            if howTo == 'RAW_LON':
                rawLon = self.NMEAStrToFloat (item)

            # LON_DIR  char    E or W (if W, lon is negative)
            if howTo == 'LON_DIR':
                offset = NMEADefs.newsOffset.get (item, gpsNAN)
                rawLon = self.NMEADegMinToDecDeg (rawLon) * offset
                if abs (rawLon) <= 180.0:
                    dataOut['GPS_LON']  = rawLon

            # FIX_QUA  int     See fixQuality list above
            #                  ex: 2         = DGPS Fix
            if howTo == 'FIX_QUA':
                if item.isdigit():
                    i = int (item)
                    if i < 9:
                        dataOut['FIX_QUA'] = NMEADefs.fixQuality[i]

            # NUM_SAT  int Number of tracked satellites (<10 may have leading 0)
            #              ex: 08        = 8 satellites
            if howTo == 'NUM_SAT':
                if item.isdigit():
                    item                = int (item)
                    if i < 25:
                        dataOut['NUM_SAT'] = i

            # HOR_DOP  double  Relative accuracy of horizontal position (in m)
            #                  ex: 0.9       = .9 meters HDOP
            if howTo == 'HOR_DOP':
                f                       = self.NMEAStrToFloat (item)
                if f != gpsNAN:
                    dataOut['HDOP']     = f

            # ALT_MSL  double  Altitude above mean sea level (MSL)
            #                  ex: 545.4     = 545.4 alt MSL
            if howTo == 'ALT_MSL':
                rawAlt = self.NMEAStrToFloat (item)
                if rawAlt != gpsNAN:
                    dataOut['ALT_MSL']  = rawAlt

            # MSLUNIT  char    Units for altitude mean sea level
            #                  M = meters
            if howTo == 'MSLUNIT':
                if item == 'M':
                    dataOut['MSLUNIT']  = item
                
            # ALT_GEO  double  Altitude above mean geoid (usually WGS84?)
            #                  ex: 342.7     = 342.7 alt MSL
            if howTo == 'MSLUNIT':
                rawAlt = self.NMEAStrToFloat (item)
                if rawAlt != gpsNAN:
                    dataOut['ALT_GEO']  = rawAlt

            # GEOUNIT  char    Units for altitude geoid
            #                  M = meters
            if howTo == 'GEOUNIT':
                if item == 'M':
                    dataOut['GEOUNIT']  = item

            # DATA_AC  char    See dataActive list above
            #                  ex: A
            if howTo == 'DATA_AC':
                da                      = NMEADefs.dataActive.get (item, 'NODT')
                if da != 'NODT':
                    dataOut['DATA_AC']  = da

            # GPSMODE  char   Mode is manual or automatic
            if howTo == 'GPSMODE':
                if item == 'M':
                    dataOut['GPSMODE']  = item
                else:
                    dataOut['GPSMODE']  = 'A'

            # FIXTYPE  char   Whether fixed
            #                 fixed to 2D or has 3D fix
            if howTo == 'FIXTYPE':
                if item == '3':
                    dataOut['FIXTYPE']  = '3D'
                elif item == '2':
                    dataOut['FIXTYPE']  = '2D'
                else:
                    dataOut['FIXTYPE']  = 'NOFIX'

            # SPD_KPH  double  Speed over ground in kph
            if howTo == 'SPD_KPH':
                kph = self.NMEAStrToFloat (item)
                if kph != gpsNAN:
                    mps = kph * 0.277777778
                    dataOut['SPD_KPH']  = kph
                    dataOut['SPD_MPS']  = mps

            # SPEEDMS  double  Speed over ground in meters per second
            if howTo == 'SPD_MPS':
                mps = self.NMEAStrToFloat(item)
                if mps != gpsNAN:
                    kph = mps / 0.277777778
                    dataOut['SPD_KPH']  = kph
                    dataOut['SPD_MPS']  = mps

            # COURSE   int  Track heading
            #               0-359 degrees
            if howTo == 'COURSE':
                if item.isdigit():
                    course = int (item)
                    if course >= 0 and course <= 359:
                        dataOut['COURSE'] = course

            # PDOP     int  Position dilution of precision
            #               0 to 9 rounded to nearest int
            if howTo == 'PDOP':
                if item.isdigit():
                    dop = int (item)
                    if dop >= 0 and dop <= 359:
                        dataOut['PDOP'] = dop

            # TDOP     int  Time dilution of precision
            #               0 to 9 rounded to nearest int
            if howTo == 'TDOP':
                if item.isdigit():
                    dop = int (item)
                    if dop >= 0 and dop <= 359:
                        dataOut['TDOP'] = dop

            # FIX_MODE char  Combine of FIX_TYPE and GPS_MODE
            #                A  = Automatic, 3 = 3D only
            if howTo == 'FIX_MODE':
                if item == '3':
                    dataOut['FIX_MODE'] = '3D'
                else:
                    dataOut['FIX_MODE'] = 'AUTO'

            # E_DATUM  int  Earth datum ID number
            if howTo == 'E_DATUM':
                if item.isdigit():
                    datum = int (item)
                    if datum == 96:
                        dataOut['E_DATUM'] = 'USERDEF'
                    if datum == 100:
                        dataOut['E_DATUM'] = 'WGS84'
                    else:
                        dataOut['E_DATUM'] = 'NOT_WGS84'

            # SM_AXIS   Relevant only if E_DATUM == USERDEF
            if howTo == 'SM_AXIS':
                pass

            # DATUMIFF   Relevant only if E_DATUM == USERDEF
            if howTo == 'DATUMIFF':
                pass

            # DATUM_DX   Relevant only if E_DATUM == USERDEF
            if howTo == 'DATUM_DX':
                pass

            # DATUM_DY   Relevant only if E_DATUM == USERDEF
            if howTo == 'DATUM_DY':
                pass

            # DATUM_DZ   Relevant only if E_DATUM == USERDEF
            if howTo == 'DATUM_DZ':
                pass

            # DIFFMODE  char  Differential mode
            if howTo == 'DIFFMODE':
                if item == 'A':
                    dataOut['DIFFMODE'] = 'AUTO'
                else:
                    dataOut['DIFFMODE'] = 'DIFF_ONLY'

            # BAUD_RT  int  NMEA 0183 Baud Rate
            if howTo == 'BAUD_RT':
                if item.isdigit():
                    baud = int (item)
                    if   baud == 3:
                        dataOut['BAUD_RT'] = '4800'
                    elif baud == 4:
                        dataOut['BAUD_RT'] = '9600'
                    elif baud == 5:
                        dataOut['BAUD_RT'] = '19200'
                    elif baud == 6:
                        dataOut['BAUD_RT'] = '300'
                    elif baud == 7:
                        dataOut['BAUD_RT'] = '600'
                    elif baud == 8:
                        dataOut['BAUD_RT'] = '38400'

            # MP_OUT  int  Measurement Pulse Output
            if howTo == 'MP_OUT':
                if item.isdigit():
                    mpo = int (item)
                    if mpo == 2:
                        dataOut['MP_OUT'] = 'ENABLED'
                    else:
                        dataOut['MP_OUT'] = 'DISABLED'

            # MP_LEN  int  Measurement Pulse Output pulse length ((n+1)* 20ms)
            if howTo == 'MP_LEN':
                if item.isdigit():
                    dataOut['MP_LEN'] = (1 + int (item)) * 20

            # DED_REC  float  Ded. Reckoning valid time 0.2 to 30.0 sec
            if howTo == 'DED_REC':
                dr = self.NMEAStrToFloat (item)
                if dr >= 0.2 or dr <= 30.0:
                    dataOut['DED_REC'] = dr

        self.dataOut = dataOut
        return NMEAErrorCode.OK
