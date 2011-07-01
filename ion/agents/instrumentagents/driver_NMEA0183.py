#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/driver_NMEA0183.py
@author Alon Yaari
@brief Driver code for connecting to NMEA0183 devices
"""

import time
from twisted.internet import defer, reactor
from serial.serialutil import SerialException
import ion.util.ionlog
from ion.core.process.process import ProcessFactory
from ion.agents.instrumentagents.instrument_driver import InstrumentDriver
from ion.agents.instrumentagents.instrument_driver import InstrumentDriverClient
from ion.agents.instrumentagents.instrument_fsm import InstrumentFSM
from ion.agents.instrumentagents.instrument_constants \
    import DriverCommand, DriverCapability, DriverStatus,\
        DriverParameter, MetadataParameter, ObservatoryState
from ion.agents.instrumentagents.instrument_constants import DriverState
from ion.agents.instrumentagents.instrument_constants import DriverEvent
from ion.agents.instrumentagents.instrument_constants import DriverAnnouncement
from ion.agents.instrumentagents.instrument_constants import DriverChannel
from ion.agents.instrumentagents.instrument_constants import BaseEnum
from ion.agents.instrumentagents.instrument_constants import InstErrorCode
from ion.core.exception import ApplicationError
import ion.util.procutils as pu
import ion.agents.instrumentagents.helper_NMEA0183 as NMEA

from twisted.protocols import basic
from twisted.internet.serialport import SerialPort
from serial import PARITY_NONE, PARITY_EVEN, PARITY_ODD
from serial import STOPBITS_ONE, STOPBITS_TWO
from serial import FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS


log = ion.util.ionlog.getLogger(__name__)

OFF = "Off"
ON = "On"
ONCE = "1"

###############################################################################
# Driver-specific constants
###############################################################################

# Device prompts
class NMEADevicePrompt(BaseEnum):
    """
    IO items
    """
    NEWLINE = '\r\n'


# Device states.
class NMEADeviceState(DriverState):
    """
    Add NMEADevice specific states here.
    """
    pass


# Device events.
class NMEADeviceEvent(DriverEvent):
    """
    Add NMEADevice specific events here.
    """
    pass


# Device commands.
class NMEADeviceCommand(DriverCommand):
    """
    Add NMEADevice specific commands here.
    """
    pass


# Device channels / transducers.
class NMEADeviceChannel(BaseEnum):
    """
    NMEADevice channels.
    """
    GPS = DriverChannel.GPS
    INSTRUMENT = DriverChannel.INSTRUMENT
    ALL = DriverChannel.ALL


# Device specific parameters.
class NMEADeviceParam(DriverParameter):
    """
    Add specific NMEA device parameters here.
    """
    GPGGA = 'GPGGA'       # Global Position Fix Position
    GPGLL = 'GPGLL'        # Global Position Latitude and Longitude
    GPRMC = 'GPRMC'        # Recommended Minimum Sentence C
    PGRMF = 'PGRMF'        # Garmin GPS Fix Data Sentence
    PGRMC = 'PGRMC'        # Garmin configuration information
    FIX_MODE = 'FIX_MODE'  # GPS fix can be 'A'utomatic or '3'D Exclusively
    ALT_MSL = 'ALT_MSL'    # Assign a valid altitude above MSL
    E_DATUM = 'E_DATUM'    # ID of earth datum:100=WGS84,96=uDef,else=NOT_WGS84
    DIFFMODE = 'DIFFMODE'  # Diff mode can be 'A'utomatic or 'D'iff exc.
    BAUD_RT = 'BAUD_RT'    # Baud 3=4800,4=9600,5=19200,6=300,7=600,8=38400
    MP_OUT = 'MP_OUT'      # Measurement pulse output 1=disabled, 2=enabled
    MP_LEN = 'MP_LEN'      # MPO length,(n+1)*20ms valid 0 to 48
    DED_REC = 'DED_REC'    # Ded. reckoning time value 0.2 to 30.0 seconds


# Device specific statuses.
class NMEADeviceStatus(DriverStatus):
    """
    Add instrument statuses here.
    """
    pass


# Device specific capabilities.
class NMEADeviceCapability(DriverCapability):
    """
    Add sbe37 capabilities here.
    """
    pass


# Device specific metadata parameters.
class NMEADeviceMetadataParameter(MetadataParameter):
    """
    Add instrument metadata types here.
    """
    pass

class GoodValues():
    baud = [300, 600, 4800, 9600, 19200, 38400, 57600]
    byteSize = [FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS]
    parity = [PARITY_NONE, PARITY_EVEN, PARITY_ODD]
    stopBits = [STOPBITS_ONE, STOPBITS_TWO]
    validChans = [NMEADeviceChannel.ALL, NMEADeviceChannel.GPS, NMEADeviceChannel.INSTRUMENT]

###############################################################################
# Helper classes.
###############################################################################


class DriverException(ApplicationError):
    """
    """


class CfgNMEADevice(object):
    """
    There are multiple methods for configuring an NMEA device.
    Currently this class converts input commands into special NMEA config
    sentences that are sent to the device.
    """
    validSet = ['GPGGA', 'GPRMC']
    cfgParams = {'GPGGA':        'OFF',
                 'GPGLL':        'OFF',
                 'GPRMC':        'OFF',
                 'PGRMF':        'OFF',
                 'PGRMC':        'OFF',
                 'FIX_MODE':     'A',
                 'ALT_MSL':      '0.0',
                 'E_DATUM':      '100',
                 'SM_AXIS':      '6360000.0',
                 'DATUMIFF':     '285.0',
                 'DATUM_DX':     '-5000.0',
                 'DATUM_DY':     '-5000.0',
                 'DATUM_DZ':     '-5000.0',
                 'DIFFMODE':     'A',
                 'BAUD_RT':      '5',
                 'VEL_FILT':     '0',
                 'MP_OUT':       '1',
                 'MP_LEN':       '0',
                 'DED_REC':      '1'}
    defParams = cfgParams

    def __init__(self):
        pass
    
    def GetCurStatus(self, param):
        return self.cfgParams.get(param)

    def SetSentences(self, toSet):
        assert(isinstance(toSet, dict)), 'Expected dict content.'
        for NMEA_CD in toSet.keys():
            self.cfgParams[NMEA_CD] = toSet[NMEA_CD]
            self.WriteToDevice(self._BuildPGRMO(NMEA_CD, toSet[NMEA_CD]))

    def SendConfigToDevice(self, toSet):
        assert(isinstance(toSet, dict)), 'Expected dict content.'
        self.WriteToDevice(self._BuildPGRMC(toSet))

    def WriteToDevice(self, toWrite):
        """
        """
        if toWrite:
            if NMEADeviceDriver.serConnection:
                NMEADeviceDriver.serConnection.write (toWrite)
                log.debug('Written to GPS: %s' % toWrite)

    def _BuildPGRMO(self, NMEA_CD, toSet):
        if NMEA_CD in self.validSet and toSet in [ON, OFF]:
            str = ['PGRMO']
            if toSet == OFF:
                setVal = 0
            else:
                setVal = 1
            if NMEA_CD == 'ALL':
                NMEA_CD = ''
                setVal += 2
            str.append(NMEA_CD)
            str.append('%d' % setVal)
            coreStr = ','.join (str)
            return '$' + coreStr + '\r\n'
        return None

    def _BuildPGRMC(self, toSet):
        """
        Assemble a PGRMC sentence to configure the GPS
        """

    # 0 --- Sentence header -----------------------------
        str = ['PGRMC']

    # 1 --- Fix Mode ------------------------------------
        x = 'FIX_MODE'
        c = toSet.get(x, self.cfgParams.get(x, self.defParams[x]))
        if 'A23'.find(c) > -1:
            str.append(c)
        else:
            str.append(self.defParams['FIX_MODE'])

    # 2 --- Alt above/below MSL -------------------------
        x = 'ALT_MSL'
        f = float(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if -1500.0 > f or f > 18000.0:
            f = 0.0
        str.append('%.1f' % f)

    # 3 --- Earth Datum Index ---------------------------
        x = 'E_DATUM'
        i = int(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if i < 0 or i > 109:
            i = 0
        str.append('%d' % i)

    # 4 --- User earth datum ----------------------------
        x = 'SM_AXIS'
        f = float(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if f < 6360000.0 or f > 6380000.0:
            f = 6360000.0
        str.append('%.3f' % f)

    # 5 --- User earth datum inverse flattening factor --
        x = 'DATUMIFF'
        f = float(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if f < 285.0 or f > 310.0:
            f = 285.0
        str.append('%.1f' % f)

    # 6 --- User earth datum delta X --------------------
        x = 'DATUM_DX'
        f = float(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if f < -5000.0 or f > 5000.0:
            f = -5000.0
        str.append('%.3f' % f)

    # 7 --- User earth datum delta Y --------------------
        x = 'DATUM_DY'
        f = float(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if f < -5000.0 or f > 5000.0:
            f = -5000.0
        str.append('%.3f' % f)
        str.append('0')

    # 8 --- User earth datum delta Z --------------------
        x = 'DATUM_DZ'
        f = float(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if f < -5000.0 or f > 5000.0:
            f = -5000.0
        str.append('%.3f' % f)

    # 9 --- Differential mode ---------------------------
        x = 'DIFFMODE'
        c = toSet.get(x, self.cfgParams.get(x, self.defParams[x]))
        if 'AD'.find(c) > -1:
            str.append(c)
        else:
            str.append (self.defParams['DIFFMODE'])

    # 10 -- NMEA 0183 Baud rate -------------------------
        x = 'BAUD_RT'
        i = int(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if i < 1 or i > 8:
            i = self.defParams['BAUD_RT']
        str.append('%d' % i)

    # 11 -- Velocity Filter -----------------------------
        x = 'VEL_FILT'
        i = int(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if i < 0 or i > 255:
            i = self.defParams["VEL_FILT"]
        str.append('%d' % i)

    # 12 -- Measurement Pulse Output --------------------
        x = 'MP_OUT'
        i = int(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if i < 1 or i > 2:
            i = self.defParams['MP_OUT']
        str.append('%d' % i)

    # 13 -- Measurement Pulse Output Length -------------
        x = 'MP_LEN'
        i = int(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if i < 0 or i > 48:
            i = self.defParams['MP_LEN']
        str.append('%d' % i)

    # 14 -- Dead reckoning valid time -------------------
        x = 'DED_REC'
        i = int(toSet.get(x, self.cfgParams.get(x, self.defParams[x])))
        if i < 1 or i > 30:
            i = self.defParams['DED_REC']
        str.append('%d' % i)

        coreStr = ','.join (str)
        return '$' + coreStr + '\r\n'

###############################################################################
# NMEA Device Driver
###############################################################################


#noinspection PyUnusedLocal
class NMEADeviceDriver(InstrumentDriver):
    """
    Implements the abstract InstrumentDriver interface for the NMEA0183 driver.
    """
    version = '0.1.0'

    serConnection = None
    data_lines = []

    @classmethod
    def get_version(cls):
        """
        Return the software version of the instrument driver.
        """
        return cls.version

    def __init__(self, *args, **kwargs):
        InstrumentDriver.__init__(self, *args, **kwargs)

        """
        The standing NMEA configuration that knows how to send itself
        to the device.
        """
        self._device_NMEA_config = CfgNMEADevice()
        
        # Reading mode
        self._serialReadMode = OFF
        # Com port of the device serial server. ex: '/dev/slave'
        self._port = None
        # Serial baud rate:  default = 9600
        self._baudrate = None
        # Number of bits:  FIVEBITS, SIXBITS, SEVENBITS, default = EIGHTBITS
        self._bytesize = None
        # Parity:  default = PARITY_NONE, PARITY_EVEN, PARITY_ODD
        self._parity = None
        # Stop bits:  default = STOPBITS_ONE, STOPBITS_TWO
        self._stopbits = None
        # MS till port timeout:  default = 0(none)
        self._timeout = None
        # Handshaking:   default = 0(off)
        self._xonxoff = None
        # Uses RTS/CTS:  default =0 (no)
        self._rtscts = None

        self._data_lines = []
        self._most_recent = {}

        # Device IO Logfile parameters. The device io log is used to precisely
        # trace comms with the device for design and debugging purposes.
        # If enabled, a new file is created each time op_connect is attempted.
        self._start_time = time.localtime()

        # List of active driver alarm conditions.
        self._alarms = []

        # Instrument state handlers
        self.state_handlers = {
            NMEADeviceState.UNCONFIGURED:   self.state_handler_unconfigured,
            NMEADeviceState.DISCONNECTED:   self.state_handler_disconnected,
            NMEADeviceState.CONNECTING:     self.state_handler_connecting,
            NMEADeviceState.DISCONNECTING:  self.state_handler_disconnecting,
            NMEADeviceState.CONNECTED:      self.state_handler_connected}

        # Instrument state machine.
        self.fsm = InstrumentFSM(NMEADeviceState,
                                  NMEADeviceEvent,
                                  self.state_handlers,
                                  NMEADeviceEvent.ENTER,
                                  NMEADeviceEvent.EXIT)

    ###########################################################################
    # State handling methods
    ###########################################################################

    @defer.inlineCallbacks
    def state_handler_unconfigured(self, event, params):
        """
        Event handler for STATE_UNCONFIGURED.
        On entering:      Driver is considered un-initialized and un-configured.
        EVENT_ENTER:      Announce trans-in then reset comm params to nulls.
        EVENT_EXIT:       Pass
        EVENT_CONFIGURE:  Set comm params then trans to DISCONNECTED state
        EVENT_INITIALIZE: Reset comm params to null values.
        """

        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer':    NMEADeviceChannel.GPS,
                       'value':         NMEADeviceState.UNCONFIGURED}
            yield self.send(self.proc_supid, 'driver_event_occurred', content)

            # Transition-in action(s)
            self._initialize()

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.INITIALIZE:
            self._initialize()

        elif event == NMEADeviceEvent.CONFIGURE:
            success = self._configure(params)
            if InstErrorCode.is_ok(success):
                next_state = NMEADeviceState.DISCONNECTED

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success, next_state, result))

    @defer.inlineCallbacks
    def state_handler_disconnected(self, event, params):
        """
        Event handler for STATE_DISCONNECTED.
        On entering:      Driver is initialized and configured but not connected
        EVENT_ENTER:      Pass.
        EVENT_EXIT:       Pass.
        EVENT_INITIALIZE: Transition to STATE_UNCONFIGURED.
        EVENT_CONNECT:    Transition to STATE_CONNECTING.
        """
        
        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.DISCONNECTED}

            yield self.send(self.proc_supid, 'driver_event_occurred', content)

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.INITIALIZE:
            next_state = NMEADeviceState.UNCONFIGURED

        elif event == NMEADeviceEvent.CONNECT:
            next_state = NMEADeviceState.CONNECTING

        elif event == NMEADeviceEvent.CONFIGURE:
            success = self._configure(params)

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success, next_state, result))

    @defer.inlineCallbacks
    def state_handler_connecting(self, event, params):
        """
        Event handler for STATE_CONNECTING.
        On entering:               Driver is initialized and configured but not
                                   connected
        EVENT_ENTER:               Attempt to establish connection.
        EVENT_EXIT:                Pass
        EVENT_CONNECTION_COMPLETE: Transition to STATE_UPDATE_PARAMS.
        EVENT_CONNECTION_FAILED:   Transition to STATE_DISCONNECTED.
        """

        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.CONNECTING}
            yield self.send(self.proc_supid, 'driver_event_occurred', content)

            # Transition-in action(s)
            yield self._getConnected()

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.CONNECTION_COMPLETE:
            # Verify a valid connection and if so, updated params then switch to connected state
            if NMEADeviceDriver.serConnection:
                yield self._update_initial_params()
                next_state = NMEADeviceState.CONNECTED
            else:
                success = InstErrorCode.DRIVER_CONNECT_FAILED
                next_state = NMEADeviceState.DISCONNECTED

        elif event == NMEADeviceEvent.CONNECTION_FAILED:
            success = InstErrorCode.DRIVER_CONNECT_FAILED
            next_state = NMEADeviceState.DISCONNECTED

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success, next_state, result))

    @defer.inlineCallbacks
    def state_handler_disconnecting(self, event, params):
        """
        Event handler for STATE_DISCONNECTING.
        On entering:               Driver MIGHT be connected to the instrument
        EVENT_ENTER:               Attempt to close connection to instrument.
        EVENT_EXIT:                Pass
        EVENT_DISCONNECT_COMPLETE: Switch to STATE_DISCONNECTED.
        """

        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.DISCONNECTING}
            yield self.send(self.proc_supid, 'driver_event_occurred', content)

            # Transition into the state
            if NMEADeviceDriver.serConnection:
                yield self._getDisconnected()

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.DISCONNECT_COMPLETE:
            next_state = NMEADeviceState.DISCONNECTED

        elif event == NMEADeviceEvent.DATA_RECEIVED:
            pass

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success, next_state, result))

    @defer.inlineCallbacks
    def state_handler_connected(self, event, params):
        """
        Event handler for STATE_CONNECTED.
        On entering:            Assume driver is connected to the instrument
        EVENT_ENTER:            Fire deferred in connection request on initial
                                entry. If a command is queued, switch to
                                command specific state for handling.
        EVENT_EXIT:             Pass
        EVENT_DISCONNECT:       Transition to STATE_DISCONNECTING.
        EVENT_COMMAND_RECEIVED: If a command is queued, switch to command
                                specific state for handling.
        EVENT_DATA_RECEIVED:    Pass
        """
        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:

            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.CONNECTED}
            yield self.send(self.proc_supid, 'driver_event_occurred', content)

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.DISCONNECT:
            next_state = NMEADeviceState.DISCONNECTING

        elif event == NMEADeviceEvent.GET:
            result =  self._get_parameters(params)
            success = result['success']
            result = result['result']

        elif event == NMEADeviceEvent.SET:
            result = self._set_parameters(params)
            success = result['success']
            result = result['result']
            # Publish any config changes caused by a successful set
            if any(result.values()):
                log.debug('Driver: Device configuration modified by set')
                config = self._get_parameters([(NMEADeviceChannel.GPS,
                                                NMEADeviceParam.ALL)])
                content = {'type': DriverAnnouncement.CONFIG_CHANGE,
                           'transducer': NMEADeviceChannel.GPS,
                           'value': config}
                yield self.send(self.proc_supid,'driver_event_occurred', content)

        elif event == NMEADeviceEvent.EXECUTE:
            # params is a single command list, already checked for channels
            # and sanitized in op_execute
            if params[0] == NMEADeviceCommand.ACQUIRE_SAMPLE:
                log.debug('ACQUIRE SAMPLE called %d' % len(self._most_recent))
                turnedOn = False

                # At least one valid sentence should be turned on
                for cd in self._device_NMEA_config.validSet:
                    if self._device_NMEA_config.cfgParams[cd] == ON:
                        turnedOn = True
                if turnedOn:
                    result = yield self._acquire_sample()
                    success = result['success']
                    result = result['result']
                    if len(result) > 0:
                        log.debug('Acquired sample: %s' % result)
                        content = {'type': DriverAnnouncement.DATA_RECEIVED,
                                   'transducer': NMEADeviceChannel.GPS,
                                   'value': result}
                        yield self.send(self.proc_supid,
                                        'driver_event_occurred',
                                        content)

                    else:
                        log.debug('Acquire Sample had no data to return')
                else:
                    log.debug('Acquire Sample could not return data; all GPS output is off')

            if params[0] == NMEADeviceCommand.START_AUTO_SAMPLING:
                self._serialReadMode = ON    # Continually acquire lines

            if params[0] == NMEADeviceCommand.STOP_AUTO_SAMPLING:
                self._serialReadMode = OFF    # Stop continually acquiring lines

        elif event == NMEADeviceEvent.DATA_RECEIVED:
            while self._data_lines:
                nmeaLine = self._data_lines.pop()
                if len(nmeaLine) > 0:

                    # This is where NMEA data is published
                    if self._serialReadMode == ON:
                        log.debug('Streaming data published: %s' % nmeaLine)
                        content = {'type': DriverAnnouncement.DATA_RECEIVED,
                                   'transducer': NMEADeviceChannel.GPS,
                                   'value': nmeaLine}
                        yield self.send(self.proc_supid,
                                        'driver_event_occurred',
                                        content)

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success, next_state, result))

    @defer.inlineCallbacks
    def state_handler_update_params(self, event, params):
        """
        Event handler for STATE_UPDATE_PARAMS.
        On entering: Assumed that driver is connected with the instrument
        Events handled:
            EVENT_ENTER:         Clear variables
            EVENT_EXIT:          Clear variables
            EVENT_PROMPTED:      Pass
            EVENT_DATA_RECEIVED: Pass
        """

        yield
        log.debug("UPDATE PARAMS handler")

        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:

            log.debug("UPDATE PARAMS handler: ENTER")

            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                        'value': NMEADeviceState.UPDATE_PARAMS}
            yield self.send(self.proc_supid, 'driver_event_occurred', content)

            log.debug("UPDATE PARAMS handler: sent state change")

            # Transition-in action(s)
            # TODO:  Send command to NMEA device with parameter changes
            next_state = NMEADeviceState.CONNECTED

        elif event == NMEADeviceEvent.EXIT:
            # TODO: Handle response when NMEA commands are sent
            # Announce the config change to agent. This assumes that param
            # updates occur one-to-one with config changes.
            # paramdict = self.get_parameter_dict()
            # content = {'type': riverAnnouncement.CONFIG_CHANGE,
            #            'transducer': NMEADeviceChannel.GPS,
            #            'value': paramdict}
            # self.send(self.proc_supid, 'driver_event_occurred', content)
            pass
        
        elif event == NMEADeviceEvent.PROMPTED:
            pass

        elif event == NMEADeviceEvent.DATA_RECEIVED:
            pass

        else:
            success = InstErrorCode.INCORRECT_STATE

        log.debug("UPDATE PARAMS handler returning : success = %s and next_state = %s",
                  success, next_state)
        
        defer.returnValue((success, next_state, result))

    @defer.inlineCallbacks
    def state_handler_set(self, event, params):
        """
        Event handler for STATE_SET.
        On entering:         Driver does not necessarily need to be connected
                             to the instrument
        EVENT_ENTER:         Init command variables, populate device command
                             buffer and start looping wakeup.
        EVENT_EXIT:          Clear command and data buffers.
                             Reply is sent as part of update params state.
        EVENT_PROMPTED:      Write next set command to device.
                             Validate previous set command with data buffer.
                             Switch to STATE_UPDATE_PARAMS if done.
        EVENT_DATA_RECEIVED: Pass.
        """

        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:

            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer':    NMEADeviceChannel.GPS,
                        'value':        NMEADeviceState.UPDATE_PARAMS}
            yield self.send(self.proc_supid, 'driver_event_occurred', content)

            # Transition-in action(s)

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.PROMPTED:
            pass

        elif event == NMEADeviceEvent.DATA_RECEIVED:
            pass

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success, next_state, result))

    ###########################################################################
    # Process lifecycle methods.
    ###########################################################################

    @defer.inlineCallbacks
    def plc_init(self):
        """
        Process lifecycle initialization.
        """

        yield

        # Set initial state.
        self.fsm.start(NMEADeviceState.UNCONFIGURED)

    @defer.inlineCallbacks
    def plc_terminate(self):
        """
        Process lifecycle termination.
        """

        yield

    ###########################################################################
    # Communications methods.
    ###########################################################################

    @defer.inlineCallbacks
    def _getConnected(self):
        """
        Called by this class to establish connection with the device.
        Send    EVENT_CONNECTION_COMPLETE   if successful or
                EVENT_CONNECTION_FAILED     if not successful.
        """
        connectionResult = NMEADeviceEvent.CONNECTION_COMPLETE
        try:
            log.debug("Driver is attempting serial connection to device....")
            NMEADeviceDriver.serConnection =  SerialPort(self._protocol,
                                              self._port,
                                              reactor,
                                              baudrate=self._baudrate,
                                              bytesize=self._bytesize,
                                              parity=self._parity,
                                              stopbits=self._stopbits,
                                              timeout=self._timeout,
                                              xonxoff=self._xonxoff,
                                              rtscts=self._rtscts)
        except SerialException, e:
            log.debug("Driver's Serial connection failed: %s", e)
            connectionResult = NMEADeviceEvent.CONNECTION_FAILED
        log.debug("Serial connection result: %s", connectionResult)
        (success, result) = yield self.fsm.on_event_async(connectionResult)

    def _update_initial_params(self):
        """
        Sets configuration parameters on connection with the device
        """
        self._device_NMEA_config.SetSentences({'GPRMC': ON})
        self._device_NMEA_config.SetSentences({'GPGGA': ON})

        # Now configure all the default settings
        self._device_NMEA_config.SendConfigToDevice(self._device_NMEA_config.cfgParams)

    @defer.inlineCallbacks
    def _getDisconnected(self):
        """
        Called by this class to close the connection to the device.
        """
        connectionResult = NMEADeviceEvent.DISCONNECT_COMPLETE
        try:
            log.debug("Attempting to disconnect serial....")
            if NMEADeviceDriver.serConnection:
                NMEADeviceDriver.serConnection.loseConnection()
        except SerialException, e:
            log.debug("Serial disconnection failed: %s", e)
            log.debug("Sending event: %s", NMEADeviceEvent.CONNECTION_COMPLETE)
            connectionResult = NMEADeviceEvent.CONNECTION_COMPLETE

        log.debug("Serial connection result: %s", connectionResult)
        (success, result) = yield self.fsm.on_event_async(connectionResult)
        
    def gotDisconnected(self, instrument):
        """
        Called by the twisted framework when the connection is closed.
        Send an EVENT_DISCONNECT_COMPLETE.
        """
        log.info("Driver has disconnected from the device")
        NMEADeviceDriver.serConnection = None
        self.fsm.on_event_async(NMEADeviceEvent.DISCONNECT_COMPLETE)

    ###########################################################################
    # Agent interface methods.
    ###########################################################################

    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute a driver command. Commands may be common or specific to the
        device, with specific commands known through knowledge of the device
        or a previous get_capabilities query.
        @param content A dict with channels and command lists and optional
            timeout:        {'channels': [chan_arg, ..., chan_arg],
                            'command': [command, arg, ..., argN]),
                            'timeout': timeout}
        @retval A reply message with a dict
            {'success': success,
            'result': {chan_arg:(success, command_specific_values), ...,
            chan_arg:(success, command_specific_values)}}
        """
        assert(isinstance(content, dict)), 'Expected dict content.'

        # Set up reply dict, get required parameters from message content.
        reply = {'success': None, 'result': None}
        command = content.get('command', None)
        channels = content.get('channels', None)
        timeout = content.get('timeout', None)

        # Fail if required parameters are absent
        if not command:
            reply['success'] = InstErrorCode.REQUIRED_PARAMETER
            yield self.reply_ok(msg, reply)
            return

        if not channels:
            reply['success'] = InstErrorCode.REQUIRED_PARAMETER
            yield self.reply_ok(msg, reply)
            return

        assert(isinstance(command,(list, tuple)))
        assert(all(map(lambda x: isinstance(x, str), command)))
        assert(isinstance(channels,(list, tuple)))
        assert(all(map(lambda x: isinstance(x, str), channels)))

        if timeout is not None:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
            pass

        # Fail if command or channels not valid for this instrument
        if not NMEADeviceCommand.has(command[0]):
            reply['success'] = InstErrorCode.UNKNOWN_COMMAND
            yield self.reply_ok(msg, reply)
            return

        for chan in channels:
            if not NMEADeviceChannel.has(chan):
                reply['success'] = InstErrorCode.UNKNOWN_CHANNEL
                yield self.reply_ok(msg, reply)
                return

        # Reaching here means we parse the command
        drv_cmd = command[0]

        if drv_cmd in [NMEADeviceCommand.ACQUIRE_SAMPLE,
                       NMEADeviceCommand.START_AUTO_SAMPLING,
                       NMEADeviceCommand.STOP_AUTO_SAMPLING]:
            # Fail if the channel is not set properly.
            if len(channels) > 1 or channels[0] != (NMEADeviceChannel.GPS):
                reply['success'] = InstErrorCode.INVALID_CHANNEL
                yield self.reply_ok(msg, reply)
                return
            else:
                # Send an execute event and setup reply            
                (success,result) = yield self.fsm.on_event_async(NMEADeviceEvent.EXECUTE, command)
                reply['success'] = success
                reply['result'] = result

        # Process test command.
        elif drv_cmd in [NMEADeviceCommand.TEST,
                         NMEADeviceCommand.CALIBRATE,
                         NMEADeviceCommand.RESET,
                         NMEADeviceCommand.TEST_ERRORS]:
            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg, reply)
            return

        else:
            # The command is properly handled in the above clause.
            reply['success'] = InstErrorCode.INVALID_COMMAND
            yield self.reply_ok(msg, reply)
            return

        yield self.reply_ok(msg,reply)        


    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        Get configuration parameters from the device.
        @param content A dict with a params list and optional timeout:
            {'params':[(chan_arg,param_arg),...,(chan_arg,param_arg)],
            'timeout':timeout}.
        @retval A reply message with a dict
            {'success':success,'result':{(chan_arg,param_arg):(success,val),...
                ,(chan_arg,param_arg):(success,val)}}
        """
        assert(isinstance(content, dict)), 'Expected dict content.'
        params = content.get('params', None)
        assert(isinstance(params,(list, tuple))), 'Expected list or tuple params.'
        timeout = content.get('timeout', None)
        if timeout:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
        reply = {'success': None, 'result': None}

        # Send get event and set up for a reply
        (success, result) = yield self.fsm.on_event_async(NMEADeviceEvent.GET, params)
        reply['success'] = success
        reply['result'] = result
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_set(self, content, headers, msg):
        """
        Set parameters to the device.
        @param content A dict containing a params dict and optional timeout:
            {'params':{(chan_arg,param_arg):val,...,(chan_arg,param_arg):val},
            'timeout':timeout}.
        @retval Reply message with a dict
            {'success':success,'result':
                {(chan_arg,param_arg):success,...,chan_arg,param_arg):success}}.
        """
        assert(isinstance(content, dict)), 'Expected dict content'
        params = content.get('params', None)
        assert(isinstance(params, dict)), 'Expected dict of param/values'
        timeout = content.get('timeout', None)
        if timeout:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
        reply = {'success': None, 'result': None}

        # Send get event and set up for a reply
        (success, result) = yield self.fsm.on_event_async(NMEADeviceEvent.SET, params)
        reply['success'] = success
        reply['result'] = result
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_execute_direct(self, content, headers, msg):
        """
        Execute untranslated commands on the device.
        @param content A dict with a bytestring containing the raw device
            commands and optional timeout: {'bytes':bytes,'timeout':timeout}
        @retval Reply message with a dict containing success value and
            raw bytes result.
            {'success':success,'result':result}.
        """
        assert(isinstance(content,dict)), 'Expected dict content.'
        params = content.get('bytes',None)
        assert(isinstance(params,str)), 'Expected bytes string.'

        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

        # The method is not implemented.
        reply = {'success':InstErrorCode.NOT_IMPLEMENTED,'result':None}
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_get_metadata(self, content, headers, msg):
        """
        Retrieve metadata for the device, its transducers and parameters.
        @param content A dict containing a params list and optional timeout:
            {'params':[(chan_arg,param_arg,meta_arg),...,
          (chan_arg,param_arg,meta_arg)],
            'timeout':timeout}.
        @retval Reply message with a dict {'success':success,'result':
                {(chan_arg,param_arg,meta_arg):(success,val),...,
                chan_arg,param_arg,meta_arg):(success,val)}}.
        """

        assert(isinstance(content,dict)), 'Expected dict content.'
        params = content.get('params',None)
        assert(isinstance(params,(list,tuple))), 'Expected list params.'
        assert(all(map(lambda x:isinstance(x,tuple),params))), \
            'Expected tuple arguments'

        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

        # The method is not implemented.
        reply = {'success':InstErrorCode.NOT_IMPLEMENTED,'result':None}
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_get_status(self, content, headers, msg):     # XX
        """
        Obtain the status of the device. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param content A dict containing a params list and an optional timeout.
            {'params':[(chan_arg,status_arg),...,(chan_arg,status_arg)],
            'timeout':timeout}
        @retval A reply message with a dict
            {'success':success,'result':{(chan_arg,status_arg):(success,val),
                ...,chan_arg,status_arg):(success,val)}}.
        """

        assert(isinstance(content, dict)), 'Expected dict content.'
        params = content.get('params', None)
        assert(isinstance(params,(list, tuple))), 'Expected list or tuple params.'
        assert(all(map(lambda x:isinstance(x, tuple), params))), \
            'Expected tuple arguments'

        # Timeout not implemented for this op.
        timeout = content.get('timeout', None)
        if timeout:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'

        reply = self._get_status(params)
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_get_capabilities(self, content, headers, msg):
        """
        Obtain the capabilities of the device, including available commands,
            parameters, channels and statuses, both common and device specific.
        @param content A dict with capabilities params and optional timeout:
            {'params':[cap_arg,...,cap_arg],'timeout':timeout}.
        @retval A reply message with a dict
            {'success':success,'result':{cap_arg:(success,val),
                ...,cap_arg:(success,val)}}.
         """

        assert(isinstance(content,dict)), 'Expected dict content.'
        params = content.get('params',None)
        assert(isinstance(params,(list,tuple))), 'Expected list or tuple params.'

        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

        # The method is not implemented.
        reply = self._get_capabilities(params)
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_initialize(self, content, headers, msg):
        """
        Restore driver to a default, unconfigured state.
        @param content A dict with optional timeout: {'timeout':timeout}.
        @retval A reply message with a dict {'success':success,'result':None}.
        """
        # Timeout not implemented for this op.
        timeout = content.get('timeout', None)
        if timeout is not None:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'

        # Set up the reply and fire an EVENT_INITIALIZE.
        reply = {'success': None, 'result': None}
        success = self.fsm.on_event_async(NMEADeviceEvent.INITIALIZE)

        # Set success and send reply.
        # Unsuccessful initialize means event is not handled in the cur. state.
        if not success:
            reply['success'] = InstErrorCode.INCORRECT_STATE
        else:
            reply['success'] = InstErrorCode.OK

        yield self.reply_ok(msg, reply)

        
    @defer.inlineCallbacks
    def op_configure(self, content, headers, msg):
        """
        Configure the driver to establish communication with the device.
        @param content a dict containing required and optional config params.
        @retval A reply message dict {'success': success, 'result': content}.
        """

        assert(isinstance(content, dict)), 'Expected dict content.'
        params = content.get('params', None)
        assert(isinstance(params, dict)), 'Expected dict params.'

        # Timeout not implemented for this op.
        timeout = content.get('timeout', None)
        if timeout is not None:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
            pass

        # Set up the reply message and validate the configuration parameters.
        # Reply with the error message if the parameters not valid.
        reply = {'success': None, 'result': params}
        #reply['success'] = self._validate_configuration(params)

        #if InstErrorCode.is_error(reply['success']):
        #    yield self.reply_ok(msg, reply)
        #    return

        # Fire EVENT_CONFIGURE with the validated configuration parameters.
        # Set the error message if  event is not handled in the current state.
        (success, result) = yield self.fsm.on_event_async(NMEADeviceEvent.CONFIGURE, params)
        reply['success'] = success
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_connect(self, content, headers, msg):
        """
        Establish connection to the device.
        @param content A dict with optional timeout: {'timeout':timeout}.
        @retval A dict {'success': success, 'result': None} giving the
            success status of the connect operation.
        """

        # Timeout not implemented for this op.
        timeout = content.get('timeout', None)
        if timeout:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
            pass
        
        reply = {'success':None, 'result':None}

        (success, result) = yield self.fsm.on_event_async(NMEADeviceEvent.CONNECT)
        reply['success'] = success
        reply['result'] = result
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_disconnect(self, content, headers, msg):
        """
        Close connection to the device.
        @retval A dict {'success': success, 'result': None} giving the
        success status of the disconnect operation.
        """
        # Timeout not implemented for this op.
        timeout = content.get('timeout', None)
        if timeout is not None:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
            pass

        reply = {'success':None,'result':None}

        # Send a disconnect event and setup the reply.
        (success,result) = yield self.fsm.on_event_async(NMEADeviceEvent.DISCONNECT)
        
        reply['success'] = success
        reply['result'] = result        
        yield self.reply_ok(msg, reply)

###########################################################################
# Nonstandard interface methods.
###########################################################################

    @defer.inlineCallbacks
    def op_get_state(self, content, headers, msg):
        """
        Retrieve the current state of the driver.
        @retval The current instrument state, from sbe37_state_list
        (See ion.agents.instrumentagents.instrument_agent_constants.device_state_list
            for the common states.)
        """

        # Get current state from the state machine and reply.
        cur_state = self.fsm.get_current_state()
        yield self.reply_ok(msg, cur_state)

###########################################################################
# Nonpublic methods.
###########################################################################
    def _debug_print(self, event, data=None):
        """
        Dump state and event status to stdio.
        """
        debugStr = "%s  %s" % (self.fsm.current_state, event)
        if isinstance(data, dict):
            for (key, val) in data.iteritems():
                debugStr += str(key) + '  ' + str(val)
            if data is not None:
                debugStr += data
            log.debug(debugStr)

    def _configure(self, params):
        """
        Set configuration parameters required for device connection to be est..
        @param params a dict of named configuration parameters
            {'param1': val1, ..., 'paramN': valN}.
        @retval True if parameters successfully set; False otherwise.
        """

        # Validate configuration.
        configCheck = self._validate_configuration(params)
        if InstErrorCode.is_error(configCheck):
            return configCheck

        # Set configuration parameters.
        self._port = params['port']
        self._baudrate = params['baudrate']
        self._bytesize = params.get('bytesize', None)
        if not self._bytesize:
            self._bytesize = EIGHTBITS
        self._parity = params.get('parity', None)
        if not self._parity:
            self._parity = PARITY_NONE
        self._stopbits = params.get('stopbits', None)
        if not self._stopbits:
            self._stopbits = STOPBITS_ONE
        self._timeout = params.get('timeout', None)
        if not self._timeout:
            self._timeout = 0
        self._xonxoff = params.get('xonxoff', None)
        if not self._xonxoff:
            self._xonxoff = 0
        self._rtscts = params.get('rtscts', None)
        if not self._rtscts:
            self._rtscts = 0
        return configCheck

    def _validate_configuration(self, params):
        """
        Validate the configuration is valid.
        @param params a dict of named configuration parameters
            {'param1': val1,..., 'paramN': valN}.
        @retval A success/fail value.
        """
        _port = params.get('port', None)
        _baudrate = params.get('baudrate', None)
        _bytesize = params.get('bytesize', None)
        _parity = params.get('parity', None)
        _stopbits = params.get('stopbits', None)
        _timeout = params.get('timeout', None)
        _xonxoff = params.get('xonxoff', None)
        _rtscts = params.get('rtscts', None)

        # fail if missing a required parameter.
        if not _port or not _baudrate:
            return InstErrorCode.REQUIRED_PARAMETER

        # Validate port name
            # Bad port name is caught with TRY on opening serial port
            # if not valid it returns InstErrorCode.INVALID_PARAM_VALUE

        if _baudrate and _baudrate not in GoodValues.baud:
            return InstErrorCode.INVALID_PARAM_VALUE
        if _bytesize and _bytesize not in GoodValues.byteSize :
            return InstErrorCode.INVALID_PARAM_VALUE
        if _parity and _parity not in GoodValues.parity:
            return InstErrorCode.INVALID_PARAM_VALUE
        if _stopbits and _stopbits not in GoodValues.stopBits:
            return InstErrorCode.INVALID_PARAM_VALUE
        if _timeout and not isinstance(_timeout, int):
            return InstErrorCode.INVALID_PARAM_VALUE
        if _xonxoff and _xonxoff not in [0, 1]:
            return InstErrorCode.INVALID_PARAM_VALUE
        if _rtscts and _rtscts not in [0, 1]:
            return InstErrorCode.INVALID_PARAM_VALUE
        return InstErrorCode.OK

    def _initialize(self):
        """
        Set the configuration to an initialized, unconfigured state.
        """
        self._protocol = NMEA0183Protocol(self)
        self._serialReadMode = OFF  # Ignore incoming NMEA lines
        self._device_NMEA_config.cfgParams = self._device_NMEA_config.defParams
        self._port = None
        self._baudrate = None
        self._bytesize = None
        self._parity = None
        self._stopbits = None
        self._timeout = None
        self._xonxoff = None
        self._rtscts = None

    def _get_status(self, params):
        """
        Get the instrument and channel status.
        @params A list of(channel,status) keys.
        @retval A dict containing success and status results:
            {'success':success,'result':
            {(chan,arg):(success,val),...,(chan,arg):(success,val)}}
        """
        # Set up the reply message.
        reply = {'success': None, 'result': None}
        result = {}
        get_errors = False

        for(chan, arg) in params:
            if NMEADeviceChannel.has(chan) and NMEADeviceStatus.has(arg):
                if chan in GoodValues.validChans:
                    chan = NMEADeviceChannel.GPS
                    ok = InstErrorCode.OK
                    all = (arg == NMEADeviceStatus.ALL)
                    if arg == NMEADeviceStatus.DRIVER_STATE or all:
                        result[(chan, NMEADeviceStatus.DRIVER_STATE)]\
                            = (ok, self.fsm.get_current_state())
                    if arg == NMEADeviceStatus.OBSERVATORY_STATE or all:
                        result[(chan, NMEADeviceStatus.OBSERVATORY_STATE)]\
                            = (ok, self._get_observatory_state())
                    if arg == NMEADeviceStatus.DRIVER_ALARMS or all:
                        result[(chan, NMEADeviceStatus.DRIVER_ALARMS)]\
                            = (ok, self._alarms)
                    if arg == NMEADeviceStatus.DRIVER_VERSION or all:
                        result[(chan, NMEADeviceStatus.DRIVER_VERSION)]\
                            = (ok, self.get_version())
                else:
                    result[(chan, arg)] = (InstErrorCode.INVALID_CHANNEL, chan)

            # Status or channel key or both invalid.
            else:
                result[(chan, arg)] = (InstErrorCode.INVALID_STATUS, None)
                get_errors = True
        reply['result'] = result

        # Set the overall error state.
        if get_errors:
            reply['success'] = InstErrorCode.GET_DEVICE_ERR
        else:
            reply['success'] = InstErrorCode.OK
        return reply

    def _get_capabilities(self,params):
        """
        Return the driver capabilities.
        @param params a list of capability arguments.
        @retval reply message with the requested capability results.
        """
        # Set up the reply message.
        reply = {'success': None, 'result': None}
        result = {}
        get_errors = False

        for arg in params:
            if NMEADeviceCapability.has(arg):
                all = (arg == NMEADeviceCapability.DEVICE_ALL)

                if arg == NMEADeviceCapability.DEVICE_COMMANDS or all:
                    result[NMEADeviceCapability.DEVICE_COMMANDS] = \
                      (InstErrorCode.OK, NMEADeviceCommand.list())

                if arg == NMEADeviceCapability.DEVICE_METADATA or all:
                    result[NMEADeviceCapability.DEVICE_METADATA] = \
                      (InstErrorCode.OK, NMEADeviceMetadataParameter.list())

                if arg == NMEADeviceCapability.DEVICE_PARAMS or all:
                    result[NMEADeviceCapability.DEVICE_PARAMS] = \
                      (InstErrorCode.OK, self._device_NMEA_config.defParams.keys())

                if arg == NMEADeviceCapability.DEVICE_STATUSES or all:
                    result[NMEADeviceCapability.DEVICE_STATUSES] = \
                      (InstErrorCode.OK, NMEADeviceStatus.list())

                if arg == NMEADeviceCapability.DEVICE_CHANNELS or all:
                    result[NMEADeviceCapability.DEVICE_CHANNELS] = \
                      (InstErrorCode.OK, NMEADeviceChannel.list())

            else:
                result[arg] =(InstErrorCode.INVALID_CAPABILITY,None)
                get_errors = True

        if get_errors:
            reply['success'] = InstErrorCode.GET_DEVICE_ERR
        else:
            reply['success'] = InstErrorCode.OK
        reply['result'] = result

        return reply

    def _get_observatory_state(self):
        """
        Return the observatory state of the instrument.
        """

        curstate = self.fsm.get_current_state()
        if curstate == NMEADeviceState.DISCONNECTED:
            return ObservatoryState.NONE

        elif curstate == NMEADeviceState.CONNECTING:
            return ObservatoryState.NONE

        elif curstate == NMEADeviceState.DISCONNECTING:
            return ObservatoryState.NONE

        elif curstate == NMEADeviceState.CONNECTED and self._serialReadMode == ONCE:
            return ObservatoryState.ACQUIRING

        elif curstate == NMEADeviceState.CONNECTED and self._serialReadMode == ON:
            return ObservatoryState.STREAMING

        elif curstate == NMEADeviceState.CONNECTED:
            return ObservatoryState.STANDBY

        elif curstate == NMEADeviceState.UNCONFIGURED:
            return ObservatoryState.NONE

        else:
            return ObservatoryState.UNKNOWN

    def get_parameter_dict(self):
        """
        Return a dict with all driver parameters.
        """
        paramdict = dict(map(lambda x:(x[0], x[1]['value']),
                             self._device_NMEA_config.defParams.items()))
        return paramdict

    ###########################################################################
    # Other.
    ###########################################################################

    @defer.inlineCallbacks
    def _acquire_sample(self):
        """
        Acquire a data sample in acquire sample polled mode.
        """
        reply = {'success':None, 'result':None}
        self._data_lines = []

        # Wait until enough data is populated
        relevant = 0
        found = 0
        while relevant and relevant - found != 0:
            yield pu.asleep(0.5)
            for cd in self._device_NMEA_config.validSet:
                if self._device_NMEA_config.cfgParams[cd] == ON:
                    relevant += 1
                    if self._most_recent.has(cd) and len (self._most_recent[cd]) > 0:
                        found += 1
        reply['success'] = InstErrorCode.OK
        samples = []
        for cd in self._device_NMEA_config.validSet:
            if self._device_NMEA_config.cfgParams[cd] == ON:
                samples.append(self._most_recent[cd])
        reply['result'] = samples
        defer.returnValue(reply)


    def _set_parameters(self, params):
        """
        Set named parameters to device hardware.
            @param params A dict containing (InstrumentChannel,InstrumentParameter)
                tuples as keys.
            @retval reply dict {'success':InstErrorCode,
                'result':{(chan,param):InstErrorCode,...,(chan,param):InstErrorCode}
        """
        reply = {'success':None, 'result':None}
        result = {}
        set_errors = False

        log.debug("*** params to handle: %s", params)
        for (chan, param) in params.keys():
            val = params[(chan, param)]
            if (val == None) or (val == ''):
                continue
            if self._device_NMEA_config.defParams.get(param):
                if param in self._device_NMEA_config.validSet:
                    # Do PGRMO stuff here
                    if val in [ON, OFF]:
                        log.debug("*** on/off val: %s", val)
                        self._device_NMEA_config.SetSentences({param: val})
                        self._device_NMEA_config.cfgParams[param] = val
                        result[(chan, param)] = InstErrorCode.OK
                    else:
                        result[(chan, param)] = InstErrorCode.BAD_DRIVER_COMMAND
                        set_errors = True
                else:
                    # Doing PGRMC stuff here
                    if param in self._device_NMEA_config.cfgParams.keys():
                        log.debug("*** PGRMC stuff param: %s", param)
                        self._device_NMEA_config.SendConfigToDevice({param: val})
                        self._device_NMEA_config.cfgParams[param] = val
                        result[(chan, param)] = InstErrorCode.OK
                    else:
                        result[(chan, param)] = InstErrorCode.BAD_DRIVER_COMMAND
                        set_errors = True
            else:
                result[(chan, param)] = InstErrorCode.INVALID_PARAMETER
                set_errors = True
        if set_errors:
            reply['success'] = InstErrorCode.SET_DEVICE_ERR
        else:
            reply['success'] = InstErrorCode.OK
        reply['result'] = result
        return reply

    def _set_local_parameters(self, nmeaData):
        """
        Sets local parameters based on a parameters sentence from the device
        """
        for nmeaKey in nmeaData.keys():
            if self._device_NMEA_config.defParams.get(nmeaKey):
                self._device_NMEA_config.cfgParams[nmeaKey] = nmeaData[nmeaKey]

    def _get_parameters(self, params):
        """
        Get named parameter values.
        @param params a list of(chan_arg,param_arg) pairs, where chan_arg
            is a valid device channel specifier(see sbe37_channel_list and
            driver_channel_list), and param_arg is a named parameter for that
            channel.
        @retval A dict {'success':success,
            'result':{(chan_arg,param_arg):(success,val),...,
              (chan_arg,param_arg):(success,val)}
        """
        reply = dict (success = None, result = None)
        result = {}
        gotErrors = False
        gpsChan = NMEADeviceChannel.GPS

        # TODO: Separate parameters from .GPS into .INSTRUMENT
        log.debug (params)
        for(chan, param) in params:
            if chan in GoodValues.validChans:
                if chan == NMEADeviceChannel.ALL or chan == gpsChan:
                    if param == NMEADeviceParam.ALL:
                        for (key, val) in self._device_NMEA_config.cfgParams.iteritems():
                            result[(gpsChan, key)] = (InstErrorCode.OK, val)
                    elif self._device_NMEA_config.cfgParams.has_key(param):
                        val = self._device_NMEA_config.cfgParams[param]
                        result[(gpsChan, param)] = (InstErrorCode.OK, val)
                    else:
                        result[(chan, param)] = (InstErrorCode.INVALID_PARAMETER, None)
                        gotErrors = True
                else:
                    result[(chan, param)] = (InstErrorCode.INVALID_CHANNEL, None)
                    gotErrors = True
            else:
                result[(chan, param)] = (InstErrorCode.INVALID_CHANNEL, chan)
                gotErrors = True

        # Set up reply success and return.
        if gotErrors:
            reply['success'] = InstErrorCode.GET_DEVICE_ERR
        else:
            reply['success'] = InstErrorCode.OK
        reply['result'] = result
        return reply

    @defer.inlineCallbacks
    def gotData(self, data):
        """
        Called by the NMEA0183Protocol whenever a line of data is received.
        """
        yield
        log.debug("GOTDATA Received from GPS: %s", data)
        nmea = NMEA.NMEAString(data)
        nmeaLine = nmea.GetNMEAData()
        if NMEA.NMEAErrorCode.is_ok(nmea.IsValid()):
            NMEA_CD = nmeaLine['NMEA_CD']

            # Handle configuration data
            if NMEA_CD == 'PGRMC':
                self._set_local_parameters(nmeaLine)

            # PGRMO commands are simply echoed commands driver sent to GPS
            elif NMEA_CD == 'PGRMO':
                pass

            # Handle non-configuration data sentences
            elif NMEA_CD in self._device_NMEA_config.cfgParams:
                self._most_recent[NMEA_CD] = nmeaLine
                self._data_lines.append(nmeaLine)
            self.fsm.on_event_async(NMEADeviceEvent.DATA_RECEIVED)
            
class NMEA0183Protocol(basic.LineReceiver):

    def __init__(self, parent):
        self.parent = parent

    def lineReceived(self, data):
        """
        Called by the twisted framework when a serial line is received.
        Override of basic.LineReceiver method.
        Takes serial line from serial port and sends it through
        the parsing pipeline.
        Sends EVENT_DATA_RECEIVED if a good NMEA line came in.
        """
        if len(data) > 1:
            self.parent.gotData(data)
        #nmeaLine = NMEA.NMEAString(data)
        #lineStatus = nmeaLine.IsValid()

        #if NMEA.NMEAErrorCode.is_ok(lineStatus):

            # If a valid sentence was received:
            #       - Store the sentence
            #       - send a data received event
            #NMEADeviceDriver._data_lines.insert(0, nmeaLine)
            #NMEADeviceDriver.fsm.on_event_async(NMEADeviceEvent.DATA_RECEIVED)
            #log.debug("NMEA received: %s", nmeaLine)

class NMEADeviceDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """

# Spawn of the process using the module name
#noinspection PyUnboundLocalVariable
factory = ProcessFactory(NMEADeviceDriver)
