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
DEBUG_PRINT = (True, False)[0]
IO_LOG = (True, False)[1]
IO_LOG_DIR = '/Users/edwardhunter/Documents/Dev/code/logfiles/'


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


class DeviceCommandSpecification:
    """
    A translation unit of a op_* function call into a sequence of low level
    device commands for use by the appropriate state handler for writing the
    commands to the device, managing the construction of a reply and firing the
    deferred returned by the op_* call upon completion.
    """

    def __init__(self, command):
        self.command = command
        self.device_command_buffer = None
        self.previous_key = None
        self.errors = False
        self.deferred = None
        self.timeoutf = None
        self.reply = {'success': None, 'result': {}}

    def do_reply(self):
        """
        Fire the command deferred with reply.
        """

        if self.timeoutf != None:
            self.timeoutf.cancel()
            self.timeoutf = None
        if self.deferred != None:
            d, self.deferred = self.deferred, None
            d.callback(self.reply)

    def do_timeout(self):
        """
        Fire the command deferred with timeout error.
        """

        reply = {'success': InstErrorCode.TIMEOUT, 'result': {}}
        if self.deferred != None:
            d, self.deferred = self.deferred, None
            d.callback(reply)
        self.timeoutf = None

    def set_success(self, success_val, fail_val):
        """
        Set the overall command success message according to any
        accumulated errors.
        """

        if self.errors:
            self.reply['success'] = fail_val
        else:
            self.reply['success'] = success_val

    def set_previous_result(self, prev_result):
        """
        For composite device commands that set a success or other result with
        each device command response prompt, use this function to build
        up the composite result.
        """

        if self.previous_key != None:
            self.reply['result'][self.previous_key] = prev_result


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

        # Deferred that handles blocking on connect messages
        self._connection_complete_deferred = None

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
        self._logfile = None
        self._start_time = time.localtime()
        self._logfile_name = ('NMEA0183_driver_io_log_%i_%i_%i.txt'
                                        % (self._start_time[3],
                                           self._start_time[4],
                                           self._start_time[5]))
        self._logfile_path = IO_LOG_DIR + self._logfile_name

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

    def state_handler_unconfigured(self, event, params):
        """
        Event handler for STATE_UNCONFIGURED.
        On entering:      Driver is considered un-intialized and un-configured.
        EVENT_ENTER:      Announce trans-in then reset comm params to nulls.
        EVENT_EXIT:       Pass
        EVENT_CONFIGURE:  Set comm params then trans to DISCONNECTED state
        EVENT_INITIALIZE: Reset comm params to null values.
        """

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer':    NMEADeviceChannel.GPS,
                       'value':         NMEADeviceState.UNCONFIGURED}
            self.send(self.proc_supid, 'driver_event_occurred', content)

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

        return(success, next_state)

    def state_handler_disconnected(self, event, params):
        """
        Event handler for STATE_DISCONNECTED.
        On entering:      Driver is initialized and configured but not connected
        EVENT_ENTER:      Pass.
        EVENT_EXIT:       Pass.
        EVENT_INITIALIZE: Transition to STATE_UNCONFIGURED.
        EVENT_CONNECT:    Transition to STATE_CONNECTING.
        """
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.DISCONNECTED}

            self.send(self.proc_supid, 'driver_event_occurred', content)

            # If we enter a disconnect state with the connection complete
            # deferred defined, then we are entering from a previous connection
            # in response to a disconnect comment. Fire the deferred with
            # reply to indicate successful disconnect.

            print '          *****_connection_complete_deferred: %s' % self._connection_complete_deferred
            if self._connection_complete_deferred:
                d, self._connection_complete_deferred = \
                    self._connection_complete_deferred, None
                reply = {'success': InstErrorCode.OK, 'result': None}
                d.callback(reply)

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.INITIALIZE:
            next_state = NMEADeviceState.UNCONFIGURED

        elif event == NMEADeviceEvent.CONNECT:
            next_state = NMEADeviceState.CONNECTING

        else:
            success = InstErrorCode.INCORRECT_STATE

        return(success, next_state)

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

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.CONNECTING}
            self.send(self.proc_supid, 'driver_event_occurred', content)

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

        return(success, next_state)

    def state_handler_disconnecting(self, event, params):
        """
        Event handler for STATE_DISCONNECTING.
        On entering:               Driver MIGHT be connected to the instrument
        EVENT_ENTER:               Attempt to close connection to instrument.
        EVENT_EXIT:                Pass
        EVENT_DISCONNECT_COMPLETE: Switch to STATE_DISCONNECTED.
        """

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.DISCONNECTING}
            self.send(self.proc_supid, 'driver_event_occurred', content)

            # Transition into the state
            self.getDisconnected()

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.DISCONNECT_COMPLETE:
            next_state = NMEADeviceState.DISCONNECTED

        else:
            success = InstErrorCode.INCORRECT_STATE

        return(success, next_state)

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

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:

            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.CONNECTED}
            self.send(self.proc_supid, 'driver_event_occurred', content)

            # If we enter connected with the connection complete deferred
            # defined we are establishing the initial connection in response
            # to a connect command. Send the reply to indicate successful
            # connection.
            if self._connection_complete_deferred:
                print "          ***** CONNECTED handler: Entered with deferred"
                d, self._connection_complete_deferred = self._connection_complete_deferred,None
                reply = {'success': InstErrorCode.OK, 'result': None}
                print "          ***** CONNECTED handler: d.callback(reply): %s" % reply
                d.callback(reply)

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

        return(success, next_state)

    def state_handler_acquire_sample(self, event, params):
        """
        Event handler for STATE_ACQUIRE_SAMPLE.
        On entering:         Assume driver is connected to the instrument
        EVENT_ENTER:         Initialize data input buffer, set serial read mode
        EVENT_EXIT:          Clear data input buffer, set serial read mode
        EVENT_PROMPTED:      Pass
        EVENT_DATA_RECEIVED: Publish data
        """

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:
            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.ACQUIRE_SAMPLE}
            self.send(self.proc_supid, 'driver_event_occurred', content)

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
                self.send(self.proc_supid, 'driver_event_occurred', content)

        else:
            success = InstErrorCode.INCORRECT_STATE

        return(success, next_state)

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

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:

            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                       'value': NMEADeviceState.DISCONNECTING}
            self.send(self.proc_supid, 'driver_event_occurred', content)

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
                self.send(self.proc_supid, 'driver_event_occurred', content)

        else:
            success = InstErrorCode.INCORRECT_STATE

        return(success, next_state)

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

        print "          ***** UPDATE PARAMS handler"

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:

            print "          ***** UPDATE PARAMS handler: ENTER"

            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer': NMEADeviceChannel.GPS,
                        'value': NMEADeviceState.UPDATE_PARAMS}
            self.send(self.proc_supid, 'driver_event_occurred', content)

            print "          ***** UPDATE PARAMS handler: sent state change"

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

        print "          ***** UPDATE PARAMS handler returning : success = %s and next_state = %s'"\
                % (success, next_state)
        return(success, next_state)

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

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == NMEADeviceEvent.ENTER:

            # Announce the state change to agent.
            content = {'type': DriverAnnouncement.STATE_CHANGE,
                       'transducer':    NMEADeviceChannel.GPS,
                        'value':        NMEADeviceState.UPDATE_PARAMS}
            self.send(self.proc_supid, 'driver_event_occurred', content)

            # Transition-in action(s)

        elif event == NMEADeviceEvent.EXIT:
            pass

        elif event == NMEADeviceEvent.PROMPTED:
            pass

        elif event == NMEADeviceEvent.DATA_RECEIVED:
            pass

        else:
            success = InstErrorCode.INCORRECT_STATE

        return(success, next_state)

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
            print '          ***** Attempting serial connection....'
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
            print '          ***** Serial connection failed: %s' % e
            print '          ***** Sending event: %s' % NMEADeviceEvent.CONNECTION_FAILED
            connectionResult = NMEADeviceEvent.CONNECTION_FAILED

        print '          ***** Serial connection result: %s' % connectionResult
        yield self.fsm.on_event(connectionResult)

    def gotConnected(self, instrument):
        """
        Called from twisted framework when connection is established.
        """

        print '          ***** gotConnected was called... why??????'

    @defer.inlineCallbacks
    def getDisconnected(self):
        """
        Called by this class to close the connection to the device.
        """

        connectionResult = NMEADeviceEvent.DISCONNECT_COMPLETE

        try:
            print '          ***** Attempting to disconnect serial....'
            if self._serConnection:
                self._serConnection.loseConnection()
        except SerialException, e:
            print '          ***** Serial disconnection failed: %s' % e
            print '          ***** Sending event: %s' % NMEADeviceEvent.CONNECTION_COMPLETE
            connectionResult = NMEADeviceEvent.CONNECTION_COMPLETE

        print '          ***** Serial connection result: %s' % connectionResult
        yield self.fsm.on_event(connectionResult)
        
    def gotDisconnected(self, instrument):
        """
        Called by the twisted framework when the connection is closed.
        Send an EVENT_DISCONNECT_COMPLETE.
        """

        print '          ***** gotDiscnnected was called... why??????'
        self._serConnection = None
        self.fsm.on_event(NMEADeviceEvent.DISCONNECT_COMPLETE)

    def gotData(self, dataFrag):

        print '          ***** gotData was called... why??????'

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

        print '          ***** op_initialize'

        # Timeout not implemented for this op.
        timeout = content.get('timeout', None)
        if timeout is not None:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
            pass

        # Set up the reply and fire an EVENT_INITIALIZE.
        reply = {'success': None, 'result': None}
        success = self.fsm.on_event(NMEADeviceEvent.INITIALIZE)

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
        reply['success'] = self._validate_configuration(params)

        if InstErrorCode.is_error(reply['success']):
            yield self.reply_ok(msg, reply)
            return

        # Fire EVENT_CONFIGURE with the validated configuration parameters.
        # Set the error message if  event is not handled in the current state.
        reply['success'] = self.fsm.on_event(NMEADeviceEvent.CONFIGURE, params)
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_connect(self, content, headers, msg):
        """
        Establish connection to the device.
        @retval A dict {'success': success, 'result': None} giving the
        success status of the connect operation.
        """

        # Timeout not implemented for this op.
        timeout = content.get('timeout', None)
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
            pass

        # Open the logfile if configured.
        if IO_LOG:
            self._logfile = open(self._logfile_path, 'w', 0)

        # Create the connection complete deferred and fire EVENT_CONNECT.
        print "          ***** op_connect: calling self._process_connect"
        reply = yield self._process_connect()
        print "          ***** op_connect reply: %s" % reply

        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_disconnect(self, content, headers, msg):
        """
        Close connection to the device.
        @retval A dict {'success': success, 'result': None} giving the
        success status of the disconnect operation.
        """

        print '          ***** op_disconnect'

        # Timeout not implemented for this op.
        timeout = content.get('timeout', None)
        if timeout is not None:
            assert(isinstance(timeout, int)), 'Expected integer timeout'
            assert(timeout > 0), 'Expected positive timeout'
            pass

        # Create the connection complete deferred and fire EVENT_DISCONNECT.
        reply = yield self._process_disconnect()
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
        return True

    def _validate_configuration(self, params):
        """
        Validate the configuration is valid.
        @param params a dict of named configuration parameters
            {'param1': val1,..., 'paramN': valN}.
        @retval A success/fail value.
        """

        print '          ***** _validate_configuration'

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

    def _process_connect(self):
        """
        Process a connect command.
        @retval Deferred that will fire with a reply msg on completion
            of connect processing.
        The reply message is a dict {'success': success, 'result': None}
        """

        # Create the connection complete deferred to be fired with
        # a reply at the conclusion of the command.
        d = defer.Deferred()
        print '          ***** _process_connect assigning deferred to _connection_complete_deferred'
        self._connection_complete_deferred = d

        # Fire EVENT_CONNECT. If the event fails the state is wrong.
        print "          ***** _process_connect about to fire a CONNECT event"
        success = self.fsm.on_event(NMEADeviceEvent.CONNECT)
        print "          ***** _process_connect: returned from fsm_event with: %s" % success
        if InstErrorCode.is_error(success):
            print "          *****  _process_connect entered the if statement"
            reply = {'success': success, 'result': None}
            d, self._connection_complete_deferred = self._connection_complete_deferred, None
            d.callback(reply)
        print "          ***** _process_connect returning d: %s" % d

        return d

    def _process_disconnect(self):
        """
        Process a disconnect command.
        @retval A deferred that will fire with a reply message
            on completion of the disconnect command.
               The reply message is a dict {'success': success, 'result': None}
        """

        # Create the connection complete deferred to be fired with
        #   a reply at the conclusion of the command.
        d = defer.Deferred()
        self._connection_complete_deferred = d

        # Fire EVENT_DISCONNECT. If the event fails the state is wrong.
        success = self.fsm.on_event(NMEADeviceEvent.DISCONNECT)
        if InstErrorCode.is_error(success):
            reply = {'success': success, 'result': None}
            d, self._connection_complete_deferred = \
                self._connection_complete_deferred, None
            d.callback(reply)
        return d


    def _process_command(self, command_spec, event, timeout=0):
        """
        Process a device command.
        Append the command to the driver command buffer and fire the event
        associated with the arrival of the command.
        @param command_spec a DeviceCommandSpecification
            that describes the command to execute.
        @param event the event to handle the command.
        @param timeout the driver timeout after which the dr
        iver will cancel the command and return a timeout error.
        @retval A deferred that will be fired
            upon conclusion of the command processing
                with a command specific reply message.
        """

        print "          ***** _process_command is not yet defined!"

        # TODO: Code _process_command for NMEADevice
        reply = {'success': InstErrorCode.INCORRECT_STATE, 'result': None}
        return reply
        
        # Create the command deferred to be returned.
        d = defer.Deferred()
        command_spec.deferred = d

        # Add the command specification to the driver command buffer.
        self._driver_command_buffer.append(command_spec)

        # If a timeout is specified, define and position it for callback.
        if timeout > 0:
            def _timeoutf(cs, driver):
                driver._debug_print('in timeout func for command '
                    + str(cs.command))

                # If this command is still active apply timeout ops.
                # If this doesn't match, then the operation completed normally
                # and was already popped from the buffer.
                if len(self._driver_command_buffer) > 0:
                    if cs == self._driver_command_buffer[0]:
                        driver._debug_print('popping command')

                        # Pop the timed out command from the buffer.
                        self._driver_command_buffer.pop(0)
                        # Cancel any looping wakeup.
                        self._stop_wakeup()
                        driver._debug_print('firing timeout')
                        # Fire the reply deferred with timeout error.
                        cs.do_timeout()

            command_spec.timeoutf = \
                reactor.callLater(timeout, _timeoutf, command_spec, self)
        # Fire the command received event and return deferred.
        if self.fsm.on_event(event):
            return d
        else:
            reply = {'success': InstErrorCode.INCORRECT_STATE, 'result': None}
            del self._driver_command_buffer[-1]
            return reply

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

        print "          ***** _get_parameters"

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

        if DEBUG_PRINT:
            print self.fsm.current_state + '  ' + event
            if isinstance(data, dict):
                for(key, val) in data.iteritems():
                    print                   str(key), '  ', str(val)
            elif data != None:
                print                       data


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
            print "||||| NMEA data received: OFF"
            return

        # Write the read-in line to the IO log if the log is enabled
        if IO_LOG:
            NMEADeviceDriver._logfile.write(line)

        nmeaLine = NMEA.NMEAString(line)
        lineStatus = nmeaLine.IsValid()

        if NMEA.NMEAErrorCode.is_ok(lineStatus):

            # If a valid sentence was received:
            #       - Store the setence
            #       - send a data received event
            NMEADeviceDriver.data_lines.insert(0, nmeaLine)
            NMEADeviceDriver.fsm.on_event(NMEADeviceEvent.DATA_RECEIVED)
            print "||||| NMEA data received: %s" % nmeaLine

        if self._serialReadMode == "1":
            self._serialReadMode = "OFF"


class NMEADeviceDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """


# Spawn of the process using the module name
factory = ProcessFactory(NMEADeviceDriver)
