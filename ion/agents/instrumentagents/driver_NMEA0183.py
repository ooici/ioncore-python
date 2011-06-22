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

import ion.agents.instrumentagents.helper_NMEA0183 as NMEA

from twisted.protocols import basic
from twisted.internet.serialport import SerialPort
from serial import PARITY_NONE, PARITY_EVEN, PARITY_ODD
from serial import STOPBITS_ONE, STOPBITS_TWO
from serial import FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS


log = ion.util.ionlog.getLogger(__name__)

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
    ACQUIRE_SINGLE_NMEA_SENTENCE = 'ACQUIRE_SINGLE_NMEA_SENTENCE'
    START_STREAMING = 'START_STREAMING'
    STOP_STREAMING = 'STOP_STREAMING'


# Device channels / transducers.
class NMEADeviceChannel(BaseEnum):
    """
    NMEADevice channels.
    """
    GPS = DriverChannel.GPS


# Device specific parameters.
class NMEADeviceParam(DriverParameter):
    """
    Add specific NMEA device parameters here.
    """
    GPGGA = 'GPGGPA'       # Global Position Fix Position
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
    MP_LEN = 'MP_LEN'      # MPO length, (n+1)*20ms valid 0 to 48
    DED_REC = 'DED_REC'    # Ded. reckoning time valud 0.2 to 30.0 seconds


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
    Add insrument metadata types here.
    """
    pass

# Add a parameters enum for instrument specific params.


###############################################################################
# Helper classes.
###############################################################################


class DriverException(ApplicationError):
    """
    """


class ConfigureNMEADevice(object):
    """
    There are multiple methods for configuring an NMEA device.
    Currently this class converts input commands into special NMEA config
    sentences that are sent to the device.
    """

    def __init__(self):

        self.configParams = {'GPGGA':        'PUBLISH',
                                'GPGLL':        'NO_PUBLISH',
                                'GPRMC':        'NO_PUBLISH',
                                'PGRMF':        'ON_REQUEST',
                                'PGRMC':        'NO_PUBLISH',
                                'FIX_MODE':     'A',
                                'ALT_MSL':      '',
                                'E_DATUM':      '100',
                                'SM_AXIS':      '',
                                'DATUMIFF':     '',
                                'DATUM_DX':     '',
                                'DATUM_DY':     '',
                                'DATUM_DZ':     '',
                                'DIFFMODE':     'A',
                                'BAUD_RT':      '5',
                                'IGNORE':       '',
                                'MP_OUT':       '1',
                                'MP_LEN':       '0',
                                'DED_REC':      '1.0'}


    def GetCurStatus(self, param):

        return                  self.configParams.get(param, None)

    def SendConfigToDevice(self):
        # TODO: Publish a $PGRMC sentence to the GPS
        pass




###############################################################################
# NMEA Device Driver
###############################################################################


class NMEADeviceDriver(InstrumentDriver):
    """
    Implements the abstract InstrumentDriver interface for the NMEA0183 driver.
    """

    """
    The software version of the NMEA0183 driver.
    """
    version = '0.1.0'

    @classmethod
    def get_version(cls):
        """
        Return the software version of the instrument driver.
        """

        return                  cls.version

    def __init__(self, *args, **kwargs):

        InstrumentDriver.__init__(self, *args, **kwargs)

        # Com port of the device serial server. ex: '/dev/slave'
        self._port = None
        # Serial baud rate:  default =9600
        self._baudrate = None
        # Number of bits:  FIVEBITS, SIXBITS, SEVENBITS, default =EIGHTBITS
        self._bytesize = None
        # Parity:  default =PARITY_NONE, PARITY_EVEN, PARITY_ODD
        self._parity = None
        # Stop bits:  default =STOPBITS_ONE, STOPBITS_TWO
        self._stopbits = None
        # MS till port timeout:  default =0(none)
        self._timeout = None
        # Handshaking:   default =0(off)
        self._xonxoff = None
        # Uses RTS/CTS:  default =0(no)
        self._rtscts = None

        self._data_lines = []

        # Device IO Logfile parameters. The device io log is used to precisely
        # trace comms with the device for design and debugging purposes.
        # If enabled, a new file is created each time op_connect is attempted.
        self._start_time = time.localtime()

        # List of active driver alarm conditions.
        self._alarms = []

        # Dictionary of instrument parameters.
        self.parameters = {
            (NMEADeviceChannel.GPS, NMEADeviceParam.GPGGA):     {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.GPGLL):     {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.GPRMC):     {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.PGRMF):     {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.PGRMC):     {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.FIX_MODE):  {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.ALT_MSL):   {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.E_DATUM):   {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.DIFFMODE):  {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.BAUD_RT):   {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.MP_OUT):    {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.MP_LEN):    {'value': None},
            (NMEADeviceChannel.GPS, NMEADeviceParam.DED_REC):   {'value': None}}

        # Instrument state handlers
        self.state_handlers = {
            NMEADeviceState.UNCONFIGURED:   self.state_handler_unconfigured,
            NMEADeviceState.DISCONNECTED:   self.state_handler_disconnected,
            NMEADeviceState.CONNECTING:     self.state_handler_connecting,
            NMEADeviceState.DISCONNECTING:  self.state_handler_disconnecting,
            NMEADeviceState.CONNECTED:      self.state_handler_connected,
            NMEADeviceState.ACQUIRE_SAMPLE: self.state_handler_acquire_sample,
            NMEADeviceState.UPDATE_PARAMS:  self.state_handler_update_params,
            NMEADeviceState.SET:            self.state_handler_set,
            NMEADeviceState.AUTOSAMPLE:     self.state_handler_autosample}

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
        On entering:      Driver is considered un-intialized and un-configured.
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
            if self._configure(params):
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
            self.getConnected()

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.CONNECTION_COMPLETE:
            next_state = NMEADeviceState.CONNECTED

        elif event == NMEADeviceEvent.CONNECTION_FAILED:
            # TODO: Push error message to the agent
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
            yield self.getDisconnected()
        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.DISCONNECT_COMPLETE:
            next_state = NMEADeviceState.DISCONNECTED

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

        elif event == NMEADeviceEvent.SET:
            next_state = NMEADeviceState.SET

        elif event == NMEADeviceEvent.ACQUIRE_SAMPLE:
            next_state = NMEADeviceState.ACQUIRE_SAMPLE

        elif event == NMEADeviceEvent.START_AUTOSAMPLE:
            next_state = NMEADeviceState.AUTOSAMPLE

        elif event == NMEADeviceEvent.TEST:
            next_state = NMEADeviceState.TEST

        elif event == NMEADeviceEvent.CALIBRATE:
            next_state = NMEADeviceState.CALIBRATE

        elif event == NMEADeviceEvent.RESET:
            next_state = NMEADeviceState.RESET

        elif event == NMEADeviceEvent.DATA_RECEIVED:
            pass

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success, next_state, result))

    @defer.inlineCallbacks
    def state_handler_acquire_sample(self, event, params):
        """
        Event handler for STATE_ACQUIRE_SAMPLE.
        On entering:         Assume driver is connected to the instrument
        EVENT_ENTER:         Initialize data input buffer, set serial read mode
        EVENT_EXIT:          Clear data input buffer, set serial read mode
        EVENT_PROMPTED:      Pass
        EVENT_DATA_RECEIVED: Publish data
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
                       'value': NMEADeviceState.ACQUIRE_SAMPLE}
            yield self.send(self.proc_supid, 'driver_event_occurred', content)

            # Transition-in action(s)
            self.protocol._serialReadMode = "1"  # Acquire only 1 line and stop

        elif event == NMEADeviceEvent.EXIT:
            # Transitioning out this way means we didn't get a valid sample
            self.protocol._serialReadMode = "OFF"  # Read no more lines
            # command

        elif event == NMEADeviceEvent.PROMPTED:
            # TODO: Implement a prompted mode for NMEA0183 devices
            pass

        elif event == NMEADeviceEvent.DATA_RECEIVED:
            while self._data_lines:
                nmeaLine = self._data_lines.pop()
                if len(nmeaLine) > 0:
                    self._debug_print('Received NMEA', nmeaLine)
                    content = {'type': DriverAnnouncement.DATA_RECEIVED,
                               'transducer': NMEADeviceChannel.GPS,
                               'value': nmeaLine}
                yield self.send(self.proc_supid, 'driver_event_occurred', content)

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success, next_state, result))

    @defer.inlineCallbacks
    def state_handler_autosample(self, event, params):
        """
        Event handler for STATE_AUTOSAMPLE.
        On entering:       Driver is assumed to be connected to the instrument.
        EVENT_ENTER:       Initialize data input buffer, set serial read mode
        EVENT_EXIT:        Clear data input buffer, set serial read mode
        EVENT_PROMPTED:    Cancel wakeup, write device commands, parse output
                           and populate reply. Switch to STATE_CONNECTED.
        EVENT_STOP_AUTOSAMPLE: Clear data input buffer, set serial read mode
        EVENT_DATA_RECEIVED:   Publish data
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

            # Transition-in action(s)
            self.protocol._serialReadMode = "ON"  # Continuous-read mode

        elif event == NMEADeviceEvent.EXIT:
            self.protocol._serialReadMode = "OFF"  # Read no more lines

        elif event == NMEADeviceEvent.PROMPTED:
            pass

        elif event == NMEADeviceEvent.STOP_AUTOSAMPLE:
                self.protocol._serialReadMode = "OFF"
                next_state = NMEADeviceState.CONNECTED

        elif event == NMEADeviceEvent.DATA_RECEIVED:
            while self._data_lines:
                nmeaLine = self._data_lines.pop()
                if len(nmeaLine) > 0:
                    self._debug_print('Received NMEA', nmeaLine)
                    content = {'type': DriverAnnouncement.DATA_RECEIVED,
                               'transducer': NMEADeviceChannel.GPS,
                               'value': nmeaLine}
                yield self.send(self.proc_supid, 'driver_event_occurred', content)

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
    def getConnected(self):
        """
        Called by this class to establish connection with the device.
        Send    EVENT_CONNECTION_COMPLETE   if successful or
                EVENT_CONNECTION_FAILED     if not successful.
        """

        connectionResult = NMEADeviceEvent.CONNECTION_COMPLETE

        try:
            log.debug("Attempting serial connection....")
            self._serConnection =  SerialPort(NMEA0183Protocol(),
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
            log.debug("Serial connection failed: %s", e)
            log.debug("Sending event: %s", NMEADeviceEvent.CONNECTION_FAILED)
            connectionResult = NMEADeviceEvent.CONNECTION_FAILED

        log.debug("Serial connection result: %s", connectionResult)
        yield self.fsm.on_event_async(connectionResult)

    def gotConnected(self, instrument):
        """
        Called from twisted framework when connection is established.
        """
        assert False, 'NMEA0183_driver.gotConnected() was called... why?'

    @defer.inlineCallbacks
    def getDisconnected(self):
        """
        Called by this class to close the connection to the device.
        """

        connectionResult = NMEADeviceEvent.DISCONNECT_COMPLETE

        try:
            log.debug("Attempting to disconnect serial....")
            if self._serConnection:
                self._serConnection.loseConnection()
        except SerialException, e:
            log.debug("Serial disconnection failed: %s", e)
            log.debug("Sending event: %s", NMEADeviceEvent.CONNECTION_COMPLETE)
            connectionResult = NMEADeviceEvent.CONNECTION_COMPLETE

        log.debug("Serial connection result: %s", connectionResult)
        yield self.fsm.on_event_async(connectionResult)
        
    def gotDisconnected(self, instrument):
        """
        Called by the twisted framework when the connection is closed.
        Send an EVENT_DISCONNECT_COMPLETE.
        """

        log.warn("gotDiscnnected was called... why??")
        self._serConnection = None
        self.fsm.on_event_async(NMEADeviceEvent.DISCONNECT_COMPLETE)

    def gotData(self, dataFrag):

        assert False, 'NMEA0183_driver.gotData() was called... why?'

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
            'result': {chan_arg: (success, command_specific_values), ...,
            chan_arg: (success, command_specific_values)}}
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

        assert(isinstance(command, (list, tuple)))
        assert(all(map(lambda x: isinstance(x, str), command)))
        assert(isinstance(channels, (list, tuple)))
        assert(all(map(lambda x: isinstance(x, str), channels)))

        if timeout != None:
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

        if drv_cmd == NMEADeviceCommand.ACQUIRE_SAMPLE:

            # Create command spec and set event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = NMEADeviceEvent.ACQUIRE_SAMPLE

            # This command only applies to the instrument as a whole.
            # Fail if the channel is not set properly.
            if len(channels) > 1 or channels[0] != NMEADeviceChannel.GPS:
                reply['success'] = InstErrorCode.INVALID_CHANNEL
                yield self.reply_ok(msg, reply)
                return

            # If the command has a length greater than 2, fail.
            else:
                reply['success'] = InstErrorCode.INVALID_COMMAND
                yield self.reply_ok(msg, reply)
                return

            # Set up the reply deferred and fire the command event.
            reply = yield self._process_command(command_spec, event, timeout)

        elif drv_cmd == NMEADeviceCommand.START_AUTO_SAMPLING:

            # Create command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = NMEADeviceEvent.START_AUTOSAMPLE

            # This command only applies to the instrument as a whole.
            # Fail if channel not set properly.
            if len(channels) > 1 or channels[0] != NMEADeviceChannel.GPS:
                reply['success'] = InstErrorCode.INVALID_CHANNEL
                yield self.reply_ok(msg, reply)
                return

            # Set up the reply deferred and fire the command event.
            reply = yield self._process_command(command_spec, event, timeout)

        elif drv_cmd == NMEADeviceCommand.STOP_AUTO_SAMPLING:

            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = NMEADeviceEvent.STOP_AUTO_SAMPLING

            # This command only applies to the instrument as a whole.
            # Fail if the channel is not set properly.
            if len(channels) > 1 or channels[0] != NMEADeviceChannel.GPS:
                reply['success'] = InstErrorCode.INVALID_CHANNEL
                yield self.reply_ok(msg, reply)
                return

            # If the command has a length greater than 2, fail.
            else:
                reply['success'] = InstErrorCode.INVALID_COMMAND
                yield self.reply_ok(msg, reply)
                return

            # Set up the reply deferred and fire the command event.
            reply = yield self._process_command(command_spec, event, timeout)

        # Process test command.
        elif drv_cmd == NMEADeviceCommand.TEST:

            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = NMEADeviceEvent.TEST

            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg, reply)
            return

        # Process test command.
        elif drv_cmd == NMEADeviceCommand.CALIBRATE:

            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = NMEADeviceEvent.CALIBRATE

            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg, reply)
            return

        # Process test command.
        elif drv_cmd == NMEADeviceCommand.RESET:

            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = NMEADeviceEvent.RESET

            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg, reply)
            return

        # Process test command.
        elif drv_cmd == NMEADeviceCommand.TEST_ERRORS:

            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)

        else:

            reply['success'] = InstErrorCode.INVALID_COMMAND
            yield self.reply_ok(msg, reply)
            return

        yield self.reply_ok(msg, reply)

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

        # TODO:  Write code for op_get
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

        # TODO:  Write code for op_set
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_execute_direct(self, content, headers, msg):
        """
        Execute untraslated commands on the device.
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
        if timeout != None:
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
        if timeout != None:
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

        assert(isinstance(content,dict)), 'Expected dict content.'
        params = content.get('params',None)
        assert(isinstance(params,(list,tuple))), 'Expected list or tuple params.'
        assert(all(map(lambda x:isinstance(x,tuple),params))), \
            'Expected tuple arguments'

        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

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
        if timeout != None:
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
            pass

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
        """
        reply['success'] = self._validate_configuration(params)

        if InstErrorCode.is_error(reply['success']):
            yield self.reply_ok(msg, reply)
            return
        """
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
        if timeout != None:
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
        Retrive the current state of the driver.
        @retval The current instrument state, from sbe37_state_list
        (See
        ion.agents.instrumentagents.
                instrument_agent_constants.device_state_list
            for the common states.)
        """

        # Get current state from the state machine and reply.
        cur_state = self.fsm.current_state
        yield self.reply_ok(msg, cur_state)

###########################################################################
# Nonpublic methods.
###########################################################################

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
            return False

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

        log.debug("***** _validate_configuration: %s", params)

        # Get required parameters.
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
        # TODO: Validate valid port name
            # DOES THIS REALLY NEED TO BE DONE?
            # Bad port name is caught with TRY on opening serial port
            # if not valid...
            # return InstErrorCode.INVALID_PARAM_VALUE

        # Validate baudrate
        if _baudrate and _baudrate not in [300, 600, 4800, 9600, 19200, 38400]:
            return InstErrorCode.INVALID_PARAM_VALUE

        # Validate bytesize
        if _bytesize and _bytesize not in \
            [FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS]:
            return InstErrorCode.INVALID_PARAM_VALUE

        # Validate parity
        if _parity and _parity not in \
            [PARITY_NONE, PARITY_EVEN, PARITY_ODD]:
            return InstErrorCode.INVALID_PARAM_VALUE

        # Validate stopbits
        if _stopbits and _stopbits not in \
            [STOPBITS_ONE, STOPBITS_TWO]:
            return InstErrorCode.INVALID_PARAM_VALUE

        # Validate timeout
        if _timeout and not isinstance(_timeout, int):
            return InstErrorCode.INVALID_PARAM_VALUE

        # Validate xonxoff
        if _xonxoff and _xonxoff not in [0, 1]:
            return InstErrorCode.INVALID_PARAM_VALUE

        # Validate rtscts
        if _rtscts and _rtscts not in [0, 1]:
            return InstErrorCode.INVALID_PARAM_VALUE

        return InstErrorCode.OK

    def _initialize(self):
        """
        Set the configuration to an initialized, unconfigured state.
        """

        self.protocol = NMEA0183Protocol()
        self._port = None
        self._baudrate = None
        self._bytesize = None
        self._parity = None
        self._stopbits = None
        self._timeout = None
        self._xonxoff = None
        self._rtscts = None

    def _get_status(self,params):
        """
        Get the instrument and channel status.
        @params A list of (channel,status) keys.
        @retval A dict containing success and status results:
            {'success':success,'result':
            {(chan,arg):(success,val),...,(chan,arg):(success,val)}}
        """

        # Set up the reply message.
        reply = {'success': None, 'result': None}
        result = {}
        get_errors = False

        for (chan,arg) in params:
            if NMEADeviceChannel.has(chan) and NMEADeviceStatus.has(arg):
                # If instrument channel or all.
                if chan == NMEADeviceChannel.GPS or chan == NMEADeviceChannel.ALL:

                    if arg == NMEADeviceStatus.DRIVER_STATE or arg == NMEADeviceStatus.ALL:
                        result[(NMEADeviceChannel.GPS, NMEADeviceStatus.DRIVER_STATE)] = \
                            (InstErrorCode.OK, self._fsm.get_current_state())

                    if arg == NMEADeviceStatus.OBSERVATORY_STATE or arg == NMEADeviceStatus.ALL:
                        result[(NMEADeviceChannel.INSTRUMENT, NMEADeviceStatus.OBSERVATORY_STATE)] = \
                            (InstErrorCode.OK, self._get_observatory_state())

                    if arg == NMEADeviceStatus.DRIVER_ALARMS or arg == NMEADeviceStatus.ALL:
                        result[(NMEADeviceChannel.INSTRUMENT, NMEADeviceStatus.DRIVER_ALARMS)] = \
                            (InstErrorCode.OK, self._alarms)

                    if arg == NMEADeviceStatus.DRIVER_VERSION or arg == NMEADeviceStatus.ALL:
                        result[(NMEADeviceChannel.INSTRUMENT, NMEADeviceStatus.DRIVER_VERSION)] = \
                            (InstErrorCode.OK, self.get_version())

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

                if arg == NMEADeviceCapability.DEVICE_COMMANDS or \
                        arg == NMEADeviceCapability.DEVICE_ALL:
                    result[NMEADeviceCapability.DEVICE_COMMANDS] = \
                        (InstErrorCode.OK, NMEADeviceCommand.list())

                if arg == NMEADeviceCapability.DEVICE_METADATA or \
                        arg == NMEADeviceCapability.DEVICE_ALL:
                    result[NMEADeviceCapability.DEVICE_METADATA] = \
                        (InstErrorCode.OK, NMEADeviceMetadataParameter.list())

                if arg == NMEADeviceCapability.DEVICE_PARAMS or \
                        arg == NMEADeviceCapability.DEVICE_ALL:
                    result[NMEADeviceCapability.DEVICE_PARAMS] = \
                        (InstErrorCode.OK, self.parameters.keys())

                if arg == NMEADeviceCapability.DEVICE_STATUSES or \
                        arg == NMEADeviceCapability.DEVICE_ALL:
                    result[NMEADeviceCapability.DEVICE_STATUSES] = \
                        (InstErrorCode.OK, NMEADeviceStatus.list())

                if arg == NMEADeviceCapability.DEVICE_CHANNELS or \
                        arg == NMEADeviceCapability.DEVICE_ALL:
                    result[NMEADeviceCapability.DEVICE_CHANNELS] = \
                        (InstErrorCode.OK, NMEADeviceChannel.list())

            else:
                result[arg] = (InstErrorCode.INVALID_CAPABILITY,None)
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

        curstate = self._fsm.get_current_state()
        if curstate == NMEADeviceState.DISCONNECTED:
            return ObservatoryState.NONE

        elif curstate == NMEADeviceState.CONNECTING:
            return ObservatoryState.NONE

        elif curstate == NMEADeviceState.DISCONNECTING:
            return ObservatoryState.NONE

        elif curstate == NMEADeviceState.ACQUIRE_SAMPLE:
            return ObservatoryState.ACQUIRING

        elif curstate == NMEADeviceState.CONNECTED:
            return ObservatoryState.STANDBY

        elif curstate == NMEADeviceState.CALIBRATE:
            return ObservatoryState.CALIBRATING

        elif curstate == NMEADeviceState.AUTOSAMPLE:
            return ObservatoryState.STREAMING

        elif curstate == NMEADeviceState.SET:
            return ObservatoryState.UPDATING

        elif curstate == NMEADeviceState.TEST:
            return ObservatoryState.TESTING

        elif curstate == NMEADeviceState.UNCONFIGURED:
            return ObservatoryState.NONE

        elif curstate == NMEADeviceState.UPDATE_PARAMS:
            return ObservatoryState.UPDATING

        else:
            return ObservatoryState.UNKNOWN

#    @defer.inlineCallbacks
#    def publish(self, topic, transducer, data):
#        """
#        """
#
#        yield

    def get_parameter_dict(self):
        """
        Return a dict with all driver parameters.
        """

        paramdict = dict(map(lambda x: (x[0], x[1]['value']),
                             self.parameters.items()))
        return paramdict

###########################################################################
# Other.
###########################################################################

    def _get_parameters(self,params):
        """
        Get named parameter values.
        @param params a list of (chan_arg,param_arg) pairs, where chan_arg
            is a valid device channel specifier (see sbe37_channel_list and
            driver_channel_list), and param_arg is a named parameter for that
            channel.
        @retval A dict {'success':success,
            'result':{(chan_arg,param_arg):(success,val),...,
                (chan_arg,param_arg):(success,val)}
        """
        # Set up the reply message.
        reply = {'success': None, 'result': None}
        result = {}
        get_errors = False

        # Get the lists of channels and parameters in the driver parameters.
        keys = self.parameters.keys()
        channels_list = [row[0] for row in keys]
        params_list = [row[1] for row in keys]

        for(chan, param) in params:

            # Retrieve all parameters.
            if chan == 'all' and param == 'all':
                for(key, val) in self.parameters.iteritems():
                    result[key] = (InstErrorCode.OK, val['value'])

            elif chan == 'all':

                # Invalid parameter name.
                if param not in params_list:
                    result[(chan, param)] = \
                        (InstErrorCode.INVALID_PARAMETER, None)
                    get_errors = True

                # Retrieve a valid named parameter for all channels.
                else:
                    for(key, val) in self.parameters.iteritems():
                        if key[1] == param:
                            result[key] = (InstErrorCode.OK, val['value'])

            elif param == 'all':

                # Invalid channel name.
                if chan not in channels_list:
                    result[(chan, param)] = \
                        (InstErrorCode.INVALID_PARAMETER, None)
                    get_errors = True

                # Retrieve all parameters for a valid named channel.
                else:
                    for(key, val) in self.parameters.iteritems():
                        if key[0] == chan:
                            result[key] = (InstErrorCode.OK, val['value'])

            # Retrieve named channel-parameters
            else:

                val = self.parameters.get((chan, param), None)

                # Invalid channel or parameter name.
                if val == None:
                    result[(chan, param)] = \
                        (InstErrorCode.INVALID_PARAMETER, None)
                    get_errors = True

                # Valid channel parameter names.
                else:
                    result[(chan, param)] = \
                        (InstErrorCode.OK, val['value'])

        # Set up reply success and return.
        if get_errors:
            reply['success'] = InstErrorCode.GET_DEVICE_ERR

        else:
            reply['success'] = InstErrorCode.OK

        reply['result'] = result
        return reply

    def _debug_print(self, event, data=None):
        """
        Dump state and event status to stdio.
        """
        log.debug ("Current state: %s, Current Event: %s, Data: %s",
                   self.fsm.current_state, event, data)

class NMEA0183Protocol(basic.LineReceiver):

    def __init__(self):
        self._serialReadMode = "OFF"

    def lineReceived(self, line):
        """
        Called by the twisted framework when a serial line is received.
        Override of basic.LineReceiver method.
        Takes serial line from serial port and sends it through
        the parsing pipeline.
        Sends EVENT_DATA_RECEIVED if a good NMEA line came in.
        """
        self._serialReadMode = "ON"
        if self._serialReadMode == "OFF":
            log.info("NMEA data received: OFF")
            return

        nmeaLine = NMEA.NMEAString(line)
        lineStatus = nmeaLine.IsValid()

        if NMEA.NMEAErrorCode.is_ok(lineStatus):

            # If a valid sentence was received:
            #       - Store the setence
            #       - send a data received event
            NMEADeviceDriver.data_lines.insert(0, nmeaLine)
            NMEADeviceDriver.fsm.on_event_async(NMEADeviceEvent.DATA_RECEIVED)
            log.info("NMEA data received: %s", nmeaLine)

        if self._serialReadMode == "1":
            self._serialReadMode = "OFF"


class NMEADeviceDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """


# Spawn of the process using the module name
factory = ProcessFactory(NMEADeviceDriver)
