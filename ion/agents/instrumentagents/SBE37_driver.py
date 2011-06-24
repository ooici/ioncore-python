#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE37_driver.py
@author Edward Hunter
@brief Driver code for SeaBird SBE-37 CTD
"""

import re
import os
import time
import datetime

from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ClientCreator
from twisted.internet.error import ConnectError

import ion.util.ionlog
import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.agents.instrumentagents.instrument_connection import InstrumentConnection
from ion.agents.instrumentagents.instrument_driver import InstrumentDriver
from ion.agents.instrumentagents.instrument_driver import InstrumentDriverClient
from ion.agents.instrumentagents.instrument_fsm import InstrumentFSM
from ion.agents.instrumentagents.instrument_constants import DriverCommand
from ion.agents.instrumentagents.instrument_constants import DriverState
from ion.agents.instrumentagents.instrument_constants import DriverEvent
from ion.agents.instrumentagents.instrument_constants import DriverAnnouncement
from ion.agents.instrumentagents.instrument_constants import DriverChannel
from ion.agents.instrumentagents.instrument_constants import DriverStatus
from ion.agents.instrumentagents.instrument_constants import DriverParameter
from ion.agents.instrumentagents.instrument_constants import MetadataParameter
from ion.agents.instrumentagents.instrument_constants import DriverCapability
from ion.agents.instrumentagents.instrument_constants import ObservatoryState
from ion.agents.instrumentagents.instrument_constants import BaseEnum
from ion.agents.instrumentagents.instrument_constants import InstErrorCode
from ion.core.exception import ApplicationError

log = ion.util.ionlog.getLogger(__name__)

DEBUG_PRINT = True if os.environ.get('DEBUG_PRINT',None) == 'True' else False
IO_LOG = True if os.environ.get('IO_LOG',None) == 'True' else False
IO_LOG_DIR = os.environ.get('IO_LOG_DIR','~/')


###############################################################################
# Constants specific to the SBE37Driver. 
###############################################################################


# Device prompts.
class SBE37Prompt(BaseEnum):
    """
    SBE37 io prompts.
    """
    PROMPT = 'S>'
    NEWLINE = '\r\n'
    BAD_COMMAND = '?cmd S>'
    STOP_AUTOSAMPLE = 'S>\r\n'

# Device states.
class SBE37State(DriverState):
    """
    Add sbe37 specific states here.
    """
    pass


# Device events.
class SBE37Event(DriverEvent):
    """
    Add sbe37 specific events here.
    """
    pass


# Device commands.
class SBE37Command(DriverCommand):
    """
    Add sbe37 specific commands here.
    """
    ACQUIRE_SAMPLE_TS = 'ACQUIRE_SAMPLE_SBE37_TS'
    ACQUIRE_SAMPLE_TSR = 'ACQUIRE_SAMPLE_SBE37_TSR'
    ACQUIRE_SAMPLE_TSS = 'ACQUIRE_SAMPLE_SBE37_TSS'
    ACQUIRE_SAMPLE_TSSON = 'ACQUIRE_SAMPLE_SBE37_TSSON'
    ACQUIRE_SAMPLE_SLT = 'ACQUIRE_SAMPLE_SBE37_SLT'
    ACQUIRE_SAMPLE_SLTR = 'ACQUIRE_SAMPLE_SBE37_SLTR'
    ACQUIRE_SAMPLE_SL = 'ACQUIRE_SAMPLE_SBE37_SL'
    TEST_TT = 'TEST_SBE37_TT'
    TEST_TC = 'TEST_SBE37_TC'
    TEST_TP = 'TEST_SBE37_TP'
    TEST_TTR = 'TEST_SBE37_TTR'
    TEST_TCR = 'TEST_SBE37_TCR'
    TEST_TPR = 'TEST_SBE37_TPR'
    TEST_TR = 'TEST_SBE37_TR'


# Device channels / transducers.
class SBE37Channel(BaseEnum):
    """
    SBE37 channels.
    """
    INSTRUMENT = DriverChannel.INSTRUMENT
    TEMPERATURE = DriverChannel.TEMPERATURE
    PRESSURE = DriverChannel.PRESSURE
    CONDUCTIVITY = DriverChannel.CONDUCTIVITY
    ALL = DriverChannel.ALL

# Device specific parameters.
class SBE37Parameter(DriverParameter):
    """
    Add sbe37 specific parameters here.
    """
    OUTPUTSAL = 'OUTPUTSAL'
    OUTPUTSV = 'OUTPUTSV'
    NAVG = 'NAVG'
    SAMPLENUM = 'SAMPLENUM'
    INTERVAL = 'INTERVAL'
    STORETIME = 'STORETIME'
    TXREALTIME = 'TXREALTIME'
    SYNCMODE = 'SYNCMODE'
    SYNCWAIT = 'SYNCWAIT'
    TCALDATE = 'TCALDATE'
    TA0 = 'TA0'
    TA1 = 'TA1'
    TA2 = 'TA2'
    TA3 = 'TA3'
    CCALDATE = 'CCALDATE'
    CG = 'CG'
    CH = 'CH'
    CI = 'CI'
    CJ = 'CJ'
    WBOTC = 'WBOTC'
    CTCOR = 'CTCOR'
    CPCOR = 'CPCOR'
    PCALDATE = 'PCALDATE'
    PA0 = 'PA0'
    PA1 = 'PA1'
    PA2 = 'PA2'
    PTCA0 = 'PTCA0'
    PTCA1 = 'PTCA1'
    PTCA2 = 'PTCA2'
    PTCB0 = 'PTCB0'
    PTCB1 = 'PTCB1'
    PTCB2 = 'PTCB2'
    POFFSET = 'POFFSET'
    RCALDATE = 'RCALDATE'
    RTCA0 = 'RTCA0'
    RTCA1 = 'RTCA1'
    RTCA2 = 'RTCA2'
    
    
# Device specific statuses.
class SBE37Status(DriverStatus):
    """
    Add sbe37 statuses here.
    """
    pass


# Device specific capabilities.
class SBE37Capability(DriverCapability):
    """
    Add sbe37 capabilities here.
    """
    pass


# Device specific metadata parameters.
class SBE37MetadataParameter(MetadataParameter):
    """
    Add sbe37 metadata types here.
    """
    pass

# Add a parameters enum for sbe37 specific params.


###############################################################################
# Helper classes.
###############################################################################
    
    
class DriverException(ApplicationError):
    """
    """
    
    
class DeviceIOParser:
    """
    A class for matching a pattern in a string line of device output and
    extracting parameter or data values from it, and optionally a method for
    converting a parameter or datavalue into a string suitable for a device
    command.
    """
    def __init__(self,pattern,getval,tostring=None):
        self.pattern = pattern
        self.regex = re.compile(pattern)
        self.value = None
        self.getval = getval
        self.tostring = tostring
        
    def parse(self,line):
        
        match = self.regex.match(line)
        if match:
            return self.getval(match)
        else:
            return None
        
    
###############################################################################
# Seabird Electronics 37-SMP MicroCAT driver.
###############################################################################
        

class SBE37Driver(InstrumentDriver):
    """
    Implements the abstract InstrumentDriver interface for the SBE37.
    """
    
    """
    The software version of the SBE37 driver.
    """
    version = '0.1.0'
    
    @classmethod
    def get_version(cls):
        """
        Return the software version of the instrument agent.
        """
        return cls.version


    def __init__(self, *args, **kwargs):
        
        
        InstrumentDriver.__init__(self, *args, **kwargs)
        
        """
        The ip port of the device serial server.
        """
        self._ipport = None
        
        """
        The ip address of the device serial server.
        """
        self._ipaddr = None
                
        """
        The instrument communication protocol object for talking to the
        device serial server.
        """
        self._instrument_connection = None
                
        """
        A string holding incomming line fragment not yet terminated by a
        newline.
        """
        self._line_buffer = ''

        """
        The queue holding completed line strings for processing by state
        handlers. This holds command responses and may be of variable length
        depending on the command and state.
        """
        self._data_lines = []
                
        """
        A queue of samples collected and parsed form the output buffer
        in autonomous mode. These may be collected so they can be returned
        in the stop reply, for example.
        """
        self._sample_buffer = []
                        
        """
        The deferred that signals a prompt was acquired.
        """
        self._prompt_acquired_deferred = None
        
        """
        The deferred that signals a prompt was acquired in autosample
        mode.
        """
        self._autosample_prompt_acquired_deferred = None
        
        """
        """
        self._disconnect_complete_deferred = None
        
        """
        Device IO Logfile parameters. The device io log is used to precisely
        trace communication with the device for design and debugging purposes.
        If enabled, a new file is create everytime a op_connect is attempted.
        """
        self._logfile = None
        self._start_time = time.localtime()
        self._logfile_name = ('SBE37_driver_io_log_%i_%i_%i.txt'
                              % (self._start_time[3],self._start_time[4],
                                 self._start_time[5]))
        self._logfile_path = IO_LOG_DIR+self._logfile_name
        
        
        """
        The data pattern and regular expression used to match and parse
        sample data in both polled and autonomous mode. Sample data always
        contains the CTD data and optionally sound velocity, salinity,
        date and time, with date and time in one of two formats.
        
        Example format permutations:
         20.1308, 0.11588,    0.718,   0.6398, 1483.513
         20.1282, 0.12386,    0.812,   0.6859, 04-14-2011, 13:41:09
         20.1265, 0.13415,    0.815, 04-14-2011, 13:40:46
         20.1198, 0.12208,    0.816,   0.6758, 1483.522, 14 Apr 2011, 13:39:00
        # 20.1214, 0.13256,    0.564,   0.7365, 1483.593, 14 Apr 2011, 13:39:33         
        -273.2093,8214.78125, -205.137, 93504.1953, 04-06-2011, 13:41:34
        #-273.2093,8214.78125, -205.137, 93504.1953, 04-06-2011, 13:41:34
        # 20.2347, 0.11958,    0.420,   0.6596, 1483.848, 04-13-2011, 16:30:08
        """
        self._sample_pattern = r'^#? *(-?\d+\.\d+), *(-?\d+\.\d+), *(-?\d+\.\d+)'
        self._sample_pattern += r'(, *(-?\d+\.\d+))?(, *(-?\d+\.\d+))?'
        self._sample_pattern += r'(, *(\d+) +([a-zA-Z]+) +(\d+), *(\d+):(\d+):(\d+))?'
        self._sample_pattern += r'(, *(\d+)-(\d+)-(\d+), *(\d+):(\d+):(\d+))?'        
        self._sample_parser = DeviceIOParser(self._sample_pattern,
                                             self._get_sample)
        
        """
        A looping call that is used to periodically send a newline to the
        device to try and get the prompt.
        """
        self._wakeup_scheduler = None
        
        """
        List of active driver alarm conditions.
        """
        self._alarms = []
        
        """
        Dictionary of instrument parameters. Each parameter object knows
        how to match and read itself from a string, and write itself in a
        string format the device will accept.
        """
        self.parameters = {
            # This needs to be fixed and should be added to status, metadata or
            # somewhere else. Also need a set clock function.
            #(SBE37Channel.INSTRUMENT,'DDMMYY'):{
            #    'value': None,
            #    'parser': DeviceIOParser(
            #        r'SBE[\w-]* +V +[\w.]+ + SERIAL +NO. +\d+ +(\d+) +([a-zA-Z]+) +(\d+) +\d+:\d+:\d+',
            #        lambda match: (int(match.group(1)),self.month2int(match.group(1)),int(match.group(3))),
            #        self.date2str_mmddyy)
            #    },
            #(SBE37Channel.INSTRUMENT,'HHMMSS'):{
            #    'value': None,
            #    'parser': DeviceIOParser(
            #        r'SBE[\w-]* +V +[\w.]+ + SERIAL +NO. +\d+ +\d+ +[a-zA-Z]+ +\d+ +(\d+):(\d+):(\d+)',
            #        lambda match: ( int(match.group(1)),self.month2int(match.group(2)),int(match.group(3))),
            #        self._time_to_string)
            #    },
            # TBD how does this get handled?
            #(SBE37Channel.INSTRUMENT,'BAUD'):{
            #    'value': None,
            #    'parser': None,
            #    'default':'9600'
            #    },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.OUTPUTSAL):{
                'value': None,
                'parser': DeviceIOParser(
                    r'(do not )?output salinity with each sample',
                    lambda match : False if match.group(1) else True,
                    self._true_false_to_string)
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.OUTPUTSV):{
                'value': None,
                'parser': DeviceIOParser(
                    r'(do not )?output sound velocity with each sample',
                    lambda match : False if match.group(1) else True,
                    self._true_false_to_string)
                },
            # TBD how does this get handled?
            #(SBE37Channel.INSTRUMENT,SBE37Parameter.FORMAT):{
            #    'value': None,
            #    'parser': DeviceIOParser(
            #        r'',
            #        lambda match : 0 ),
            #    'default':1
            #    },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.NAVG):{
                'value': None,
                'parser': DeviceIOParser(
                    r'number of samples to average = (\d+)',
                    lambda match : int(match.group(1)),
                    self._int_to_string)       
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.SAMPLENUM):{
                'value': None,
                'parser': DeviceIOParser(
                    r'samplenumber = (\d+), free = \d+',
                    lambda match : int(match.group(1)),
                    self._int_to_string)       
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.INTERVAL):{
                'value': None,
                'parser': DeviceIOParser(
                    r'sample interval = (\d+) seconds',
                    lambda match : int(match.group(1)),
                    self._int_to_string)       
                 },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.STORETIME):{
                'value': None,
                'parser': DeviceIOParser(
                    r'(do not )?store time with each sample',
                    lambda match : False if match.group(1) else True,
                    self._true_false_to_string)
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.TXREALTIME):{
                'value': None,
                'parser': DeviceIOParser(
                    r'(do not )?transmit real-time data',
                    lambda match : False if match.group(1) else True,
                    self._true_false_to_string)
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.SYNCMODE):{
                'value': None,
                'parser': DeviceIOParser(
                    r'serial sync mode (enabled|disabled)',
                    lambda match : False if (match.group(1)=='disabled') else True,
                    self._true_false_to_string)
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.SYNCWAIT):{
                'value': None,
                'parser': DeviceIOParser(
                    r'wait time after serial sync sampling = (\d+) seconds',
                    lambda match : int(match.group(1)),
                    self._int_to_string)
                },
            # Should cal dates be parameters or metadata?
            (SBE37Channel.INSTRUMENT,SBE37Parameter.TCALDATE):{
                'value': None,
                'parser': DeviceIOParser(
                    r'temperature: +((\d+)-([a-zA-Z]+)-(\d+))',
                    lambda match : self._string_to_date(match.group(1),
                                                        '%d-%b-%y'),
                    self._date_to_string)
                },
            (SBE37Channel.TEMPERATURE,SBE37Parameter.TA0):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +TA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.TEMPERATURE,SBE37Parameter.TA1):{
                'value': None,
                'parser': DeviceIOParser(
                   r' +TA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.TEMPERATURE,SBE37Parameter.TA2):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +TA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.TEMPERATURE,SBE37Parameter.TA3):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +TA3 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            # Should cal dates be parameters or metadata?                
            (SBE37Channel.CONDUCTIVITY,SBE37Parameter.CCALDATE):{
                'value': None,
                'parser': DeviceIOParser(
                    r'conductivity: +((\d+)-([a-zA-Z]+)-(\d+))',
                    lambda match : self._string_to_date(match.group(1),
                                                        '%d-%b-%y'),
                    self._date_to_string)
                },
            (SBE37Channel.CONDUCTIVITY,SBE37Parameter.CG):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +G = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.CONDUCTIVITY,SBE37Parameter.CH):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +H = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.CONDUCTIVITY,SBE37Parameter.CI):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +I = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.CONDUCTIVITY,SBE37Parameter.CJ):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +J = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.CONDUCTIVITY,SBE37Parameter.WBOTC):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +WBOTC = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.CONDUCTIVITY,SBE37Parameter.CTCOR):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +CTCOR = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.CONDUCTIVITY,SBE37Parameter.CPCOR):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +CPCOR = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            # Should cal dates be parameters or metadata?                
            (SBE37Channel.PRESSURE,SBE37Parameter.PCALDATE):{
                'value': None,
                'parser': DeviceIOParser(
                    r'pressure .+ ((\d+)-([a-zA-Z]+)-(\d+))',
                    lambda match : self._string_to_date(match.group(1),
                                                        '%d-%b-%y'),
                    self._date_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PA0):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PA1):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PA2):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PTCA0):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PTCA1):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PTCA2):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PTCB0):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCSB0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PTCB1):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCSB1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.PTCB2):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCSB2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.PRESSURE,SBE37Parameter.POFFSET):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +POFFSET = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            # Should cal dates be parameters or metadata?                
            (SBE37Channel.INSTRUMENT,SBE37Parameter.RCALDATE):{
                'value': None,
                'parser': DeviceIOParser(
                    r'rtc: +((\d+)-([a-zA-Z]+)-(\d+))',
                    lambda match : self._string_to_date(match.group(1),
                                                        '%d-%b-%y'),
                    self._date_to_string)
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.RTCA0):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +RTCA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.RTCA1):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +RTCA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            (SBE37Channel.INSTRUMENT,SBE37Parameter.RTCA2):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +RTCA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                }
        }
        
        """
        Instrument state handlers
        """
        self._state_handlers = {
            SBE37State.UNCONFIGURED : self.state_handler_unconfigured,
            SBE37State.DISCONNECTED : self.state_handler_disconnected,
            SBE37State.CONNECTING : self.state_handler_connecting,
            SBE37State.DISCONNECTING : self.state_handler_disconnecting,
            SBE37State.CONNECTED : self.state_handler_connected,
            SBE37State.AUTOSAMPLE : self.state_handler_autosample
        }
        
        """
        Instrument state machine.
        """
        self._fsm = InstrumentFSM(SBE37State,SBE37Event,self._state_handlers,
                                 SBE37Event.ENTER,SBE37Event.EXIT)


    ###########################################################################
    # State handling method.
    ###########################################################################

         
    @defer.inlineCallbacks
    def state_handler_unconfigured(self,event,params):
        """
        Event handler for STATE_UNCONFIGURED.
        Events handled:
        EVENT_ENTER: Reset communication parameters to null values.
        EVENT_EXIT: Pass.
        EVENT_CONFIGURE: Set communication parameters and switch to
        STATE_DISCONNECTED if successful.
        EVENT_INITIALIZE: Reset communication parameters to null values.
        """
        
        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)
        
        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.                        
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.UNCONFIGURED}
            yield self.send(self.proc_supid,'driver_event_occurred',content)
            
            # Initialize driver configuration.
            self._initialize()

        elif event == SBE37Event.EXIT:
            pass
        
        elif event == SBE37Event.INITIALIZE:
            
            # Initialize driver configuration.
            self._initialize()

        elif event == SBE37Event.CONFIGURE:
            
            # Attempt to configure driver, switch to disconnected
            # if successful.
            success = self._configure(params)
            if InstErrorCode.is_ok(success):
                next_state = SBE37State.DISCONNECTED

        else:
            success = InstErrorCode.INCORRECT_STATE
            
        defer.returnValue((success,next_state,result))
        
    
    @defer.inlineCallbacks
    def state_handler_disconnected(self,event,params):
        """
        Event handler for STATE_DISCONNECTED.
        Events handled:
        EVENT_ENTER: Pass.
        EVENT_EXIT: Pass.        
        EVENT_INITIALIZE: Switch to STATE_UNCONFIGURED.
        EVENT_CONNECT: Switch to STATE_CONNECTING.
        """
        
        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)
        
        if event == SBE37Event.ENTER:

            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.DISCONNECTED}
            yield self.send(self.proc_supid,'driver_event_occurred',content)
            
        elif event == SBE37Event.EXIT:
            pass

        elif event == SBE37Event.INITIALIZE:
            
            # Switch back to unconfigured state.
            next_state = SBE37State.UNCONFIGURED         
                    
        elif event == SBE37Event.CONNECT:
            
            # Switch to connecting state which will initiate connection to
            # instrument hardware.
            next_state = SBE37State.CONNECTING         
        
        elif event == SBE37Event.CONFIGURE:
            
            # Attempt to alter driver configuration. Stays in current
            # connectable configuration if the attempt is invalid.
            success = self._configure(params)
        
        else:
            success = InstErrorCode.INCORRECT_STATE
            
        defer.returnValue((success,next_state,result))


    @defer.inlineCallbacks
    def state_handler_connecting(self,event,params):
        """
        Event handler for STATE_CONNECTING.
        Events handled:
        EVENT_ENTER: Attemmpt to establish connection.
        EVENT_EXIT: Pass.        
        EVENT_CONNECTION_COMPLETE: Switch to STATE_UPDATE_PARAMS.
        EVENT_CONNECTION_FAILED: Switch to STATE_DISCONNECTED.
        """
        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)
        
        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.CONNECTING}
            yield self.send(self.proc_supid,'driver_event_occurred',content)

            # Attempt to set up a tcp connection to the serial server.
            cc = ClientCreator(reactor, InstrumentConnection, self)
            try:
                self._instrument_connection = yield cc.connectTCP(self._ipaddr,
                                                          int(self._ipport))
            except ConnectError, ex:
                self._instrument_connection = None
                success = InstErrorCode.DRIVER_CONNECT_FAILED

            else:
                self._instrument_connection.transport.setTcpNoDelay(True)


        elif event == SBE37Event.EXIT:
            pass
                    
        elif event == SBE37Event.CONNECTION_COMPLETE:

            # If connection valid, update parameters and switch to connected.
            if self._instrument_connection:
                yield self._update_params()
                next_state = SBE37State.CONNECTED
                
            # else, return to disconnected.
            else:
                success = InstErrorCode.DRIVER_CONNECT_FAILED
                next_state = SBE37State.DISCONNECTED
                    
        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success,next_state,result))


    @defer.inlineCallbacks
    def state_handler_disconnecting(self,event,params):
        """
        Event handler for STATE_DISCONNECTING.
        Events handled:
        EVENT_ENTER: Attempt to close connection to instrument.
        EVENT_EXIT: Pass.        
        EVENT_DISCONNECT_COMPLETE: Switch to STATE_DISCONNECTED.
        """
        
        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)
            
        if event == SBE37Event.ENTER:

            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.DISCONNECTED}
            yield self.send(self.proc_supid,'driver_event_occurred',content)
            
            # Drop the driver connection.
            # This blocks until the framework closes the connection,
            # so the op_ call doesn't return until it is closed.
            if self._instrument_connection:
                self._disconnect_complete_deferred = defer.Deferred()
                self._instrument_connection.transport.loseConnection()
                yield self._disconnect_complete_deferred

        elif event == SBE37Event.EXIT:
            
            # Fire the disconnect deferred to end the op_ call.
            if self._disconnect_complete_deferred:
                self._disconnect_complete_deferred.callback(None)
                self._disconnect_complete_deferred = None
                    
        elif event == SBE37Event.DISCONNECT_COMPLETE:
            
            # Switch to disconnected when connection is dropped by the
            # framework.
            next_state = SBE37State.DISCONNECTED

        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success,next_state,result))


    @defer.inlineCallbacks
    def state_handler_connected(self,event,params):
        """
        Event handler for STATE_CONNECTED.
        EVENT_ENTER: Fire deferred in connection request on initial entry.
        If a command is queued, switch to command specific state for handling.
        EVENT_EXIT: Pass.
        EVENT_DISCONNECT: Switch to STATE_DISCONNECTING.
        EVENT_COMMAND_RECEIVED: If a command is queued, switch to command
        specific state for handling.
        EVENT_DATA_RECEIVED: Pass.
        """
        
        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.CONNECTED}
            yield self.send(self.proc_supid,'driver_event_occurred',content)            
            
        elif event == SBE37Event.EXIT:
            pass
            
        elif event == SBE37Event.DISCONNECT:

            # Switch to disconnecting state.            
            next_state = SBE37State.DISCONNECTING
                
            
        elif event == SBE37Event.GET:
            
            # Get specified parameters from driver and setup result.
            result = self._get_parameters(params)
            success = result['success']
            result = result['result']
        
        elif event == SBE37Event.SET:
            
            # Set specified parameters to hardware, update driver
            # parameters and setup result.
            result = yield self._set_parameters(params)
            yield self._update_params()
            success = result['success']
            result = result['result']
            
            # Do expanded set command checking here.
            
            # If any of the sets were successful, publish the
            # config change.
            if any(result.values()):
                self._debug_print('modified configuration')
                config = self._get_parameters([(SBE37Channel.ALL,
                                                SBE37Parameter.ALL)])
                content = {'type':DriverAnnouncement.CONFIG_CHANGE,
                           'transducer':SBE37Channel.INSTRUMENT,
                           'value':config}
                yield self.send(self.proc_supid,'driver_event_occurred',
                                content)
                
        elif event == SBE37Event.ACQUIRE_SAMPLE:
            
            # Acquire a data sample.
            result = yield self._acquire_sample()
            success = result['success']
            result = result['result']
                        
            # Publish result if successful.
            if len(result)>0:
                self._debug_print('received samples',result)
                content = {'type':DriverAnnouncement.DATA_RECEIVED,
                           'transducer':SBE37Channel.INSTRUMENT,'value':result}
                yield self.send(self.proc_supid,'driver_event_occurred',
                                content)                                                
            
        elif event == SBE37Event.START_AUTOSAMPLE:
            
            # Switch to autosample mode. 
            next_state = SBE37State.AUTOSAMPLE
            
        elif event == SBE37Event.TEST:
            
            # Switch to test mode.
            next_state = SBE37State.TEST
            
        elif event == SBE37Event.CALIBRATE:
            
            # Switch to calibrate mode.
            next_state = SBE37State.CALIBRATE
            
        elif event == SBE37Event.RESET:
            
            # Switch to reset mode.
            next_state = SBE37State.RESET
            
        elif event == SBE37Event.DATA_RECEIVED:
            pass
  
        else:
            success = InstErrorCode.INCORRECT_STATE

        defer.returnValue((success,next_state,result))


    @defer.inlineCallbacks
    def state_handler_autosample(self,event,params):
        """
        Event handler for STATE_AUTOSAMPLE.
        Events handled:
        EVENT_ENTER: Initialize command buffers and start looping wakeup.
        EVENT_EXIT: Clear buffers, populate and send reply.       
        EVENT_PROMPTED: Cancel wakeup, write device commands, parse output
        and populate reply. Switch to STATE_CONNECTED.
        EVENT_STOP_AUTOSAMPLE: Populate command buffer and start looping
        wakeup.
        EVENT_DATA_RECEIVED: Pass.
        """
        
        yield
        success = InstErrorCode.OK
        next_state = None
        result = None
        self._debug_print(event)

        if event == SBE37Event.ENTER:

            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.AUTOSAMPLE}
            yield self.send(self.proc_supid,'driver_event_occurred',content)                                    

            # Clear data lines and sample buffer.
            self._data_lines = []
            self._sample_buffer = []
            
            # Get the prompt and send start command without waiting for
            # response prompt.
            prompt = yield self._get_prompt()
            yield self._do_cmd_no_prompt('startnow')
                        
        elif event == SBE37Event.EXIT:            

            # Clear data lines and sample buffer.
            self._data_lines = []
            self._sample_buffer = []
                  
        elif event == SBE37Event.STOP_AUTOSAMPLE:

            # Get the autosample prompt, issue stop command, get
            # regular prompt, store results if option set, switch to
            # connected.
            prompt = yield self._get_autosample_prompt()
            prompt = yield self._do_cmd_no_prompt('stop')
            prompt = yield self._get_prompt()
            if params and 'GETDATA' in params:
                result = self._sample_buffer
            next_state = SBE37State.CONNECTED
            
        elif event == SBE37Event.DATA_RECEIVED:
            
            # Data received in autosample mode.
            # Parse line buffer for samples, store in sample buffer
            # and publish if any are found.
            samples = self._parse_sample_output()
            if len(samples)>0:
                self._sample_buffer += samples
                self._debug_print('received samples',samples)
                content = {'type':DriverAnnouncement.DATA_RECEIVED,
                           'transducer':SBE37Channel.INSTRUMENT,'value':samples}
                yield self.send(self.proc_supid,'driver_event_occurred',content)                                                
            
        else:            
            success = InstErrorCode.INCORRECT_STATE
        
        defer.returnValue((success,next_state,result))


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
        self._fsm.start(SBE37State.UNCONFIGURED)
                

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
    def _get_connected(self):
        """
        Establish connection to the device hardware with the current
        configuration.
        @retval (success, result)
        """
        
        # Initiate the connection.
        # This blocks until a connection is established.
        # Fails if in wrong state or connection fails.
        (success, result) = yield self._fsm.on_event_async(SBE37Event.CONNECT)
        
        # If connection successful, finalize connection.
        # This blocks on driver parameter intialization and switches to
        # connected state.
        if InstErrorCode.is_ok(success):
            (success, result) = yield self._fsm.on_event_async(\
                                            SBE37Event.CONNECTION_COMPLETE)
        
        defer.returnValue((success, result))

   
    @defer.inlineCallbacks
    def _get_disconnected(self):
        """
        Close connection to the device hardware.
        @retval (success, result).
        """

        # Send the disconnect event to lose the connection.
        # This fails if the state is incorrect.
        # If state is correct, this event blocks until the connection is
        # closed and the state is switched.
        (success, result) = yield self._fsm.on_event_async(\
                                                SBE37Event.DISCONNECT)            

        defer.returnValue((success, result))

        
    @defer.inlineCallbacks
    def gotConnected(self, instrument):
        """
        Called from twisted framework when connection is established.
        """
        
        yield
        """        
        if self._instrument_connection:
            self._instrument_connection.transport.setTcpNoDelay(1)
            yield self._fsm.on_event_async(SBE37Event.CONNECTION_COMPLETE)
        else:
            yield self._fsm.on_event_async(SBE37Event.CONNECTION_FAILED)
        """
         
    @defer.inlineCallbacks
    def gotDisconnected(self, instrument):
        """
        Called by the twisted framework when the connection is closed.
        Send an EVENT_DISCONNECT_COMPLETE.
        """

        # Destroy the protocol.
        # self._instrument_connection = None
        
        # Fire the disconnect complete deferred if it exists.
        # This causes disconnect op to complete.
        #if self._disconnect_complete_deferred:
        #    self._disconnect_complete_deferred.callback(None)
        #    self._disconnect_complete_deferred = None
            
        # Send the disconnect complete event.
        # This switches state if we are closing properly, and
        # allows errors to be caught by other state handlers
        # if disconnect occurs spontaneously.
        yield self._fsm.on_event_async(SBE37Event.DISCONNECT_COMPLETE)


    @defer.inlineCallbacks
    def gotData(self, dataFrag):
        """
        Called by the twisted framework when a data fragment is received.
        Update line and data buffers. Send EVENT_DATA_RECEIVED and
        EVENT_PROMPTED if a prompt is detected.
        @param dataFrag a string data fragment received.
        """
        
        yield
        
        # Write the fragment to the IO log if it is enabled.
        if IO_LOG:
            self._logfile.write(dataFrag)

        # Add the fragment to the line buffer.
        self._line_buffer += dataFrag

        new_lines = False
   
        # If the line buffer has newlines, extract complete lines and add
        # to the data buffer. Keep the tail fragment if any in the line buffer
        # to append further incomming data to.
        if SBE37Prompt.NEWLINE in self._line_buffer:
            lines = self._line_buffer.split(SBE37Prompt.NEWLINE)
            self._line_buffer = lines[-1]
            self._data_lines += lines[0:-1]
            new_lines = True

        # If the linebuffer ends with a prompt, extract and append the
        # prefix data to the data buffer. Keep the prompt in the line buffer.
        if self._line_buffer.endswith(SBE37Prompt.PROMPT):
            self._data_lines.append(self._line_buffer.replace(SBE37Prompt.PROMPT,''))
            self._line_buffer = SBE37Prompt.PROMPT
            new_lines = True

        # If the line buffer ends with a bad command prompt, extract and
        # append the prefix data to the data buffer. Keep the prompt in the
        # line buffer.
        elif self._line_buffer.endswith(SBE37Prompt.BAD_COMMAND):
            self._data_lines.append(self._line_buffer.
                                    replace(SBE37Prompt.BAD_COMMAND,''))
            self._line_buffer = SBE37Prompt.BAD_COMMAND
            new_lines = True

        # If new complete lines are detected, send an EVENT_DATA_RECEIVED.
        if new_lines and self._fsm.get_current_state() == SBE37State.AUTOSAMPLE:
            yield self._fsm.on_event_async(SBE37Event.DATA_RECEIVED)
        
        # If a normal or bad command prompt is detected, send an
        # EVENT_PROMPTED
        if self._line_buffer == SBE37Prompt.PROMPT:
            if self._prompt_acquired_deferred:
                d,self._prompt_acquired_deferred = \
                                    self._prompt_acquired_deferred, None
                self._stop_wakeup()
                d.callback(SBE37Prompt.PROMPT)
            
        elif self._line_buffer == SBE37Prompt.BAD_COMMAND:
            if self._prompt_acquired_deferred:
                d,self._prompt_acquired_deferred = \
                                    self._prompt_acquired_deferred, None
                self._stop_wakeup()
                d.callback(SBE37Prompt.BAD_COMMAND)
        
        elif self._line_buffer == '' and len(self._data_lines)>0 and \
            self._data_lines[-1] == SBE37Prompt.PROMPT:
            if self._autosample_prompt_acquired_deferred:
                d,self._autosample_prompt_acquired_deferred = \
                                    self._autosample_prompt_acquired_deferred, None
                self._stop_wakeup()
                d.callback(SBE37Prompt.STOP_AUTOSAMPLE)


    ###########################################################################
    # Agent interface methods.
    ###########################################################################


    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute a driver command. Commands may be
        common or specific to the device, with specific commands known through
        knowledge of the device or a previous get_capabilities query.
        @param content A dict with channels and command lists and optional
            timeout:
            {'channels':[chan_arg,...,chan_arg],
            'command':[command,arg,...,argN]),
            'timeout':timeout}.
        @retval A reply message with a dict
            {'success':success,
            'result':{chan_arg:(success,command_specific_values),...,
            chan_arg:(success,command_specific_values)}}. 
        """
        
        assert(isinstance(content,dict)), 'Expected dict content.'

        # Set up reply dict, get required parameters from message content.
        reply = {'success':None,'result':None}
        command = content.get('command',None)
        channels = content.get('channels',None)
        timeout = content.get('timeout',None)

        # Fail if required parameters absent.
        if not command:
            reply['success'] = InstErrorCode.REQUIRED_PARAMETER
            yield self.reply_ok(msg,reply)
            return
        if not channels:
            reply['success'] = InstErrorCode.REQUIRED_PARAMETER
            yield self.reply_ok(msg,reply)
            return

        assert(isinstance(command,(list,tuple)))        
        assert(all(map(lambda x:isinstance(x,str),command)))
        assert(isinstance(channels,(list,tuple)))        
        assert(all(map(lambda x:isinstance(x,str),channels)))
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

        # Fail if command or channels not valid for sbe37.
        if not SBE37Command.has(command[0]):
            reply['success'] = InstErrorCode.UNKNOWN_COMMAND
            yield self.reply_ok(msg,reply)
            return

        for chan in channels:
            if not SBE37Channel.has(chan):
                reply['success'] = InstErrorCode.UNKNOWN_CHANNEL
                yield self.reply_ok(msg,reply)
                return

        drv_cmd = command[0]
        
        # Process acquire sample command.
        if drv_cmd == SBE37Command.ACQUIRE_SAMPLE:

            # The acquire command only applies to the instrument as a whole.
            # Fail if the channel is not set properly.
            if len(channels)>1 or channels[0] != SBE37Channel.INSTRUMENT:
                reply['success'] = InstErrorCode.INVALID_CHANNEL
                yield self.reply_ok(msg,reply)
                return

            # Send an acquire sample event and setup reply
            (success,result) = yield self._fsm.on_event_async(SBE37Event.ACQUIRE_SAMPLE)
            reply['success'] = success
            reply['result'] = result

        # Process start autosampling command.
        elif drv_cmd == SBE37Command.START_AUTO_SAMPLING:

            # Send an start autosample event and setup reply
            (success,result) = yield self._fsm.on_event_async(SBE37Event.START_AUTOSAMPLE)
            reply['success'] = success
            reply['result'] = result

        # Process stop autosampling command.
        elif drv_cmd == SBE37Command.STOP_AUTO_SAMPLING:

            # Get optional stop parameters.
            if len(command)>1:
                params = command[1:]
            else:
                params = None

            # Send stop autosample event and setup reply.
            (success,result) = yield self._fsm.on_event_async(SBE37Event.STOP_AUTOSAMPLE,params)
            reply['success'] = success
            reply['result'] = result

        # Process test command.
        elif drv_cmd == SBE37Command.TEST:

            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg,reply)
            return

        elif drv_cmd == SBE37Command.CALIBRATE:

            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg,reply)
            return

        elif drv_cmd == SBE37Command.RESET:

            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg,reply)
            return

        else:

            # The command is properly handled in the above clause.
            reply['success'] = InstErrorCode.INVALID_COMMAND
            yield self.reply_ok(msg,reply)
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
        
        assert(isinstance(content,dict)),'Expected dict content.'
        params = content.get('params',None)
        assert(isinstance(params,(list,tuple))),'Expected list or tuple params.'


        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass
        
        reply = {'success':None,'result':None}
        
        # Send get event and setup reply.
        (success,result) = yield self._fsm.on_event_async(SBE37Event.GET,params)
        reply['success'] = success
        reply['result'] = result
        
        yield self.reply_ok(msg,reply)


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
        
        assert(isinstance(content,dict)), 'Expected dict content.'
        params = content.get('params',None)
        assert(isinstance(content,dict)), 'Expected dict params.'

        assert(all(map(lambda x: isinstance(x,(list,tuple)),
                       params.keys())),True), 'Expected list or tuple dict keys.'
        assert(all(map(lambda x: isinstance(x,str),
                       params.values())),True), 'Expected string dict values.'
        
        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass
        
        reply = {'success':None,'result':None}
        
        # Send set event and setup reply. Set handler announces config
        # changes to agent.
        (success,result) = yield self._fsm.on_event_async(SBE37Event.SET,params)
        reply['success'] = success
        reply['result'] = result
        
        yield self.reply_ok(msg,reply)


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
    def op_get_status(self, content, headers, msg):
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
        timeout = content.get('timeout',None)
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

        # Set up the reply and fire an EVENT_INITIALIZE.
        reply = {'success':None,'result':None}         
        (success, result) = yield self._fsm.on_event_async(SBE37Event.INITIALIZE)
        
        # Set success and send reply. Unsuccessful initialize means the
        # event is not handled in the current state.
        if not success:
            reply['success'] = InstErrorCode.INCORRECT_STATE

        else:
            reply['success'] = InstErrorCode.OK
 
        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_configure(self, content, headers, msg):
        """
        Configure the driver to establish communication with the device.
        @param content a dict containing a configuration dict and optional
            timeout: {'params':{'cfg_arg':cfg_val,...,'cfg_arg':cfg_val},
            'timeout':timeout}.
        @retval A reply message dict {'success':success,'result':content}.
        """
        
        assert(isinstance(content,dict)), 'Expected dict content.'
        params = content.get('params',None)
        assert(isinstance(params,dict)), 'Expected dict params.'
        
        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

        # Set up the reply message and validate the configuration parameters.
        # Reply with the error message if the parameters not valid.
        reply = {'success':None,'result':params}
        """
        reply['success'] = self._validate_configuration(params)

        if InstErrorCode.is_error(reply['success']):
            yield self.reply_ok(msg,reply)
            return
        """
        # Fire EVENT_CONFIGURE with the validated configuration parameters.
        # Set the error message if the event is not handled in the current
        # state.
        (success,result) = yield self._fsm.on_event_async(SBE37Event.CONFIGURE,params)
        reply['success'] = success

        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_connect(self, content, headers, msg):
        """
        Establish connection to the device.
        @param content A dict with optional timeout: {'timeout':timeout}.
        @retval A dict {'success':success,'result':None} giving the success
            status of the connect operation.
        """
        
        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

        # Open the logfile if configured.
        if IO_LOG:
            self._logfile = open(self._logfile_path,'w',0)

        reply = {'success':None,'result':None}

        # Successful if the state is correct and the connection
        # is successfully opened.
        (success, result) = yield self._get_connected()
            
        reply['success'] = success
        
        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_disconnect(self, content, headers, msg):
        """
        Close connection to the device.
        @param content A dict with optional timeout: {'timeout':timeout}.
        @retval A dict {'success':success,'result':None} giving the success
            status of the disconnect operation.
        """
        
        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass

        reply = {'success':None,'result':None}
        
        # Successful if the state is correct and the connection is
        # successfully closed.
        (success, result) = yield self._get_disconnected()
       
        reply['success'] = success
        
        yield self.reply_ok(msg,reply)


    ###########################################################################
    # Nonstandard interface methods.
    ###########################################################################

    @defer.inlineCallbacks
    def op_get_state(self, content, headers, msg):
        """
        Retrive the current state of the driver.
        @param content A dict with optional timeout: {'timeout':timeout}.
        @retval The current instrument state, from sbe37_state_list
        (see ion.agents.instrumentagents.instrument_agent_constants.
            device_state_list for the common states.)
        """
        
        # Get current state from the state machine and reply.
        cur_state = self._fsm.current_state
        
        yield self.reply_ok(msg, cur_state)


    ###########################################################################
    # Nonpublic op function helpers.
    ###########################################################################        


    def _configure(self,params):
        """
        Set configuration parameters required for device connection to
        be established.
        @param params a dict of named configuration parameters
            {'param1':val1,...,'paramN':valN}.
        @retval True if parameters successfully set, False otherwise.
        """
        
        # Validate configuration.
        success = self._validate_configuration(params)
        if InstErrorCode.is_error(success):
            return success

        # Set configuration parameters.
        self._ipport = params['ipport']
        self._ipaddr = params['ipaddr']

        return success


    def _validate_configuration(self,params):
        """
        Validate the configuration is valid.
        @param params a dict of named configuration parameters
            {'param1':val1,...,'paramN':valN}.
        @retval A success/fail value.
        """
        
        # Get required parameters.
        _ipport = params.get('ipport',None)
        _ipaddr = params.get('ipaddr',None)

        # fail if missing a required parameter.
        if not _ipport or not _ipaddr:
            return InstErrorCode.REQUIRED_PARAMETER

        # Validate port number.
        if not isinstance(_ipport,int) or _ipport <0 or _ipport > 65535:
            return InstErrorCode.INVALID_PARAM_VALUE
        
        # Validate ip address.
        # TODO: Add logic to veirfy string format.
        if not isinstance(_ipaddr,str): 
            return InstErrorCode.INVALID_PARAM_VALUE

        return InstErrorCode.OK

        
    def _initialize(self):
        """
        Set the configuration to an initialized, unconfigured state.
        """
        
        self._ipport = None
        self._ipaddr = None


    def _get_status(self,params):
        """
        Get the instrument and channel status.
        @params A list of (channel,status) keys.
        @retval A dict containing success and status results:
            {'success':success,'result':
            {(chan,arg):(success,val),...,(chan,arg):(success,val)}}
        """
        
        # Set up the reply message.
        reply = {'success':None,'result':None}
        result = {}
        get_errors = False

        for (chan,arg) in params:
            if SBE37Channel.has(chan) and SBE37Status.has(arg):
                # If instrument channel or all.
                if chan == SBE37Channel.INSTRUMENT or chan == SBE37Channel.ALL:
                    if arg == SBE37Status.DRIVER_STATE or \
                            arg == SBE37Status.ALL:
                        result[(SBE37Channel.INSTRUMENT,
                                SBE37Status.DRIVER_STATE)] = \
                            (InstErrorCode.OK,self._fsm.get_current_state())
                    
                    if arg == SBE37Status.OBSERVATORY_STATE or \
                            arg == SBE37Status.ALL:
                        result[(SBE37Channel.INSTRUMENT,
                                SBE37Status.OBSERVATORY_STATE)] = \
                            (InstErrorCode.OK,self._get_observatory_state())
                    
                    if arg == SBE37Status.DRIVER_ALARMS or \
                            arg == SBE37Status.ALL:
                        result[(SBE37Channel.INSTRUMENT,
                                SBE37Status.DRIVER_ALARMS)] = \
                            (InstErrorCode.OK,self._alarms)
                    
                    if arg == SBE37Status.DRIVER_VERSION or \
                            arg == SBE37Status.ALL:
                        result[(SBE37Channel.INSTRUMENT,
                                SBE37Status.DRIVER_VERSION)] = \
                            (InstErrorCode.OK,self.get_version())
    
                # If conductivity channel or all.            
                if chan == SBE37Channel.CONDUCTIVITY or\
                         chan == SBE37Channel.ALL:
                    if arg == SBE37Status.ALL:
                        pass
                        
                    # Conductivity channel does not support this status.
                    else:
                        result[(chan,arg)] = \
                            (InstErrorCode.INVALID_STATUS,None)
                        get_errors = True
                        
                if chan == SBE37Channel.PRESSURE or chan == SBE37Channel.ALL:
                    if arg == SBE37Status.ALL:
                        pass

                    # Pressure channel does not support this status.                        
                    else:
                        result[(chan,arg)] = \
                            (InstErrorCode.INVALID_STATUS,None)
                        get_errors = True
    
                # If pressure channel or all.
                if chan == SBE37Channel.TEMPERATURE or \
                         chan == SBE37Channel.ALL:
                    if arg == SBE37Status.ALL:
                        pass
                        
                    # Temp channel does not support this status.
                    else:
                        result[(chan,arg)] = \
                            (InstErrorCode.INVALID_STATUS,None)
                        get_errors = True
                 
            # Status or channel key or both invalid.       
            else:
                result[(chan,arg)] = (InstErrorCode.INVALID_STATUS,None)
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
        reply = {'success':None,'result':None}
        result = {}
        get_errors = False

        for arg in params:
            if SBE37Capability.has(arg):
                
                if arg == SBE37Capability.DEVICE_COMMANDS or \
                        arg == SBE37Capability.DEVICE_ALL:
                    result[SBE37Capability.DEVICE_COMMANDS] = \
                        (InstErrorCode.OK,SBE37Command.list())
                
                if arg == SBE37Capability.DEVICE_METADATA or \
                        arg == SBE37Capability.DEVICE_ALL:
                    result[SBE37Capability.DEVICE_METADATA] = \
                        (InstErrorCode.OK,SBE37MetadataParameter.list())
                
                if arg == SBE37Capability.DEVICE_PARAMS or \
                        arg == SBE37Capability.DEVICE_ALL:
                    result[SBE37Capability.DEVICE_PARAMS] = \
                        (InstErrorCode.OK,self.parameters.keys())
                
                if arg == SBE37Capability.DEVICE_STATUSES or \
                        arg == SBE37Capability.DEVICE_ALL:
                    result[SBE37Capability.DEVICE_STATUSES] = \
                        (InstErrorCode.OK,SBE37Status.list())
            
                if arg == SBE37Capability.DEVICE_CHANNELS or \
                        arg == SBE37Capability.DEVICE_ALL:
                    result[SBE37Capability.DEVICE_CHANNELS] = \
                        (InstErrorCode.OK,SBE37Channel.list())

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
        if curstate == SBE37State.DISCONNECTED:
            return ObservatoryState.NONE
        
        elif curstate == SBE37State.CONNECTING:
            return ObservatoryState.NONE

        elif curstate == SBE37State.DISCONNECTING:
            return ObservatoryState.NONE

        elif curstate == SBE37State.ACQUIRE_SAMPLE:
            return ObservatoryState.ACQUIRING

        elif curstate == SBE37State.CONNECTED:
            return ObservatoryState.STANDBY

        elif curstate == SBE37State.CALIBRATE:
            return ObservatoryState.CALIBRATING

        elif curstate == SBE37State.AUTOSAMPLE:
            return ObservatoryState.STREAMING

        elif curstate == SBE37State.SET:
            return ObservatoryState.UPDATING

        elif curstate == SBE37State.TEST:
            return ObservatoryState.TESTING

        elif curstate == SBE37State.UNCONFIGURED:
            return ObservatoryState.NONE

        elif curstate == SBE37State.UPDATE_PARAMS:
            return ObservatoryState.UPDATING

        else:
            return ObservatoryState.UNKNOWN

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
        reply = {'success':None,'result':None}
        result = {}
        get_errors = False

        # Get the lists of channels and parameters in the driver parameters.
        keys = self.parameters.keys()
        channels_list = [row[0] for row in keys]
        params_list = [row[1] for row in keys]

        for (chan,param) in params:
            
            # Retrieve all parameters.
            if chan == SBE37Channel.ALL and param == SBE37Parameter.ALL:
                for (key,val) in self.parameters.iteritems():
                    result[key] = (InstErrorCode.OK,val['value'])

            elif chan == SBE37Channel.ALL:

                # Invalid parameter name.
                if param not in params_list:
                    result[(chan,param)] = (InstErrorCode.INVALID_PARAMETER,None)
                    get_errors = True
                    
                # Retrieve a valid named parameter for all channels.
                else:
                    for (key,val) in self.parameters.iteritems():
                        if key[1] == param:
                            result[key] = (InstErrorCode.OK,val['value'])

            elif param == SBE37Parameter.ALL:

                # Invalid channel name.
                if chan not in channels_list:
                    result[(chan,param)] = (InstErrorCode.INVALID_PARAMETER,None)
                    get_errors = True
                    
                # Retrieve all parameters for a valid named channel.
                else:
                    for (key,val) in self.parameters.iteritems():
                        if key[0] == chan:
                            result[key] = (InstErrorCode.OK,val['value'])
                            
            # Retrieve named channel-parameters
            else:

                val = self.parameters.get((chan,param),None)

                # Invalid channel or parameter name.
                if val == None:
                    result[(chan,param)] = (InstErrorCode.INVALID_PARAMETER,None)
                    get_errors = True

                # Valid channel parameter names.
                else:
                    result[(chan,param)] = (InstErrorCode.OK,val['value'])
        
        # Set up reply success and return.
        if get_errors:
            reply['success'] = InstErrorCode.GET_DEVICE_ERR

        else:
            reply['success'] = InstErrorCode.OK

        reply['result'] = result
        return reply

    @defer.inlineCallbacks
    def _set_parameters(self,params):
        """
        Set named parameters to device hardware.
        @param params A dict containing (InsturmentChannel,InstrumentParameter)
            tuples as keys.
        @retval reply dict {'success':InstErrorCode,
            'result':{(chan,param):InstErrorCode,...,(chan,param):InstErrorCode}
        """
        
        set_errors = False
        reply = {'success':None,'result':None}
        result = {}
        
        prompt = yield self._get_prompt()
        sample_num_command = None
        for (chan,param) in params.keys():
            val = params[(chan,param)]
            if self.parameters.get((chan,param),None):
                #self._debug_print('creating command for '+chan+'.'+param)
                str_val = self.parameters[(chan,param)]['parser'].tostring(val)
                if str_val:
                    if (chan,param) == (SBE37Channel.INSTRUMENT,'SAMPLENUM'):
                        sample_num_command = param+'='+str_val
                    else:
                        command = param+'='+str_val
                        prompt = yield self._do_cmd(command)
                        #yield pu.asleep(1)
                        if prompt == SBE37Prompt.PROMPT:
                            result[(chan,param)] = InstErrorCode.OK

                        elif prompt == SBE37Prompt.BAD_COMMAND:
                            result[(chan,param)] = InstErrorCode.BAD_DRIVER_COMMAND
                            set_errors = True
                            self._debug_print('Bad device command.')

                        else:
                            result[(chan,param)] = InstErrorCode.BAD_DRIVER_COMMAND
                            set_errors = True
                            self._debug_print('unexpected prompt \
                                              in set_parameters:'+prompt+'xxx')
                            self._debug_print('sbe prompt:'+\
                                              SBE37Prompt.PROMPT+'xxx')
                            yield self._get_prompt()

                else:
                    # Parser error creating value string.
                    self._debug_print('Error creating parameter string.')
                    pass
            else:
                result[(chan,param)] = InstErrorCode.INVALID_PARAMETER
                set_errors = True

        if sample_num_command:
            prompt = yield self._do_cmd(sample_num_command)
            yield pu.asleep(1)
            if prompt == SBE37Prompt.PROMPT:
                result[(chan,param)] = InstErrorCode.OK

            elif prompt == SBE37Prompt.BAD_COMMAND:
                result[(chan,param)] = InstErrorCode.BAD_DRIVER_COMMAND
                set_errors = True
                self._debug_print('Bad device command.')

            else:
                result[(chan,param)] = InstErrorCode.BAD_DRIVER_COMMAND
                set_errors = True
                self._debug_print('unexpected prompt \
                                  in set_parameters:'+prompt+'xxx')
                self._debug_print('sbe prompt:'+SBE37Prompt.PROMPT+'xxx')
                yield self._get_prompt()
            
        if set_errors:
            reply['success'] = InstErrorCode.SET_DEVICE_ERR
        
        else:
            reply['success'] = InstErrorCode.OK
        
        reply['result'] = result
        
        defer.returnValue(reply)
        
    
    @defer.inlineCallbacks
    def _acquire_sample(self):
        """
        Acquire a data sample in polled mode.
        @retval reply dict {'success':InstErrorCode,
            'result':data sample dictionary}
        """
        
        self._debug_print('acquiring sample')
        reply = {'success':None,'result':None}        

        # Clear data lines.
        self._data_lines = []
        
        # Acquire prompt.
        prompt = yield self._get_prompt()
        
        # Write sample command waiting for prompt return.
        prompt = yield self._do_cmd('ts')
                    
        # Parse datalines for sample output.
        samples = self._parse_sample_output()
        
        # Setup the reply.
        if len(samples)==0:
            reply['success'] = InstErrorCode.ACQUIRE_SAMPLE_ERR
        else:
            reply['success'] = InstErrorCode.OK
            reply['result'] = samples
        
        # Clear data lines.    
        self._data_lines = []
        
        defer.returnValue(reply)
        
    
    @defer.inlineCallbacks    
    def _update_params(self):
        """
        Update all driver parameters with current hardware configuration.
        """

        self._debug_print('updating parameters')
        
        # Clear data lines.
        self._data_lines = []
        
        # Get prompt, issue device status command, issue device calibration
        # status command. Await prompt for each.
        prompt = yield self._get_prompt()
        prompt = yield self._do_cmd('ds')
        prompt = yield self._do_cmd('dc')
        
        # Parse data lines for all parameter values and update driver
        # parameters.
        self._read_param_values(self._data_lines)
        
        # Clear data lines.
        self._data_lines = []
        
        defer.returnValue(None)


    ###########################################################################
    # Nonpublic wakeup helpers.
    ###########################################################################        


    def _wakeup(self,wakeup_string=SBE37Prompt.NEWLINE,reps=1):
        """
        Send a wakeup attempt to the device.
        """
        
        self._debug_print('sending wakeup')
        if self._instrument_connection:
            self._instrument_connection.transport.write(wakeup_string*reps)
            self._instrument_connection.transport.doWrite()        
            

    def _start_wakeup(self,period):
        """
        Start the looping wakeup.
        """
        if self._wakeup_scheduler:
            self._stop_wakeup()
        self._wakeup_scheduler = task.LoopingCall(self._wakeup)
        self._wakeup_scheduler.start(period)

        
    def _stop_wakeup(self):
        """
        Cancel the looping wakeup.
        """
        if self._wakeup_scheduler:
            self._wakeup_scheduler.stop()
            self._wakeup_scheduler = None       


    ###########################################################################
    # Prompt and device command helpers.
    ###########################################################################        


    @defer.inlineCallbacks
    def _get_prompt(self):
        """
        Wake device and block until a current device prompt is returned.
        @retval the returned instrument prompt constant.
        """

        self._debug_print('getting prompt')
        d = defer.Deferred()
        self._prompt_acquired_deferred = d
        self._start_wakeup(2.0)
        prompt = yield d
        yield pu.asleep(.5)
        defer.returnValue(prompt)


    @defer.inlineCallbacks
    def _get_autosample_prompt(self):
        """
        Wake device in streaming mode and block until streaming prompt
        is returned.
        @retval the returned instrument streaming prompt constant.
        """

        self._debug_print('getting autosample prompt')
        d = defer.Deferred()
        self._autosample_prompt_acquired_deferred = d
        self._start_wakeup(2.0)
        prompt = yield d
        yield pu.asleep(.5)
        defer.returnValue(prompt)

    
    @defer.inlineCallbacks
    def _do_cmd(self,cmd):
        """
        Write a device command to the hardware blocking until a prompt is
        returned.
        @retval the returned instrument prompt constant.
        """
        
        self._debug_print('sending device command '+cmd)
        d = defer.Deferred()
        self._prompt_acquired_deferred = d
        
        # Write the fragment to the IO log if it is enabled.
        if IO_LOG:
            self._logfile.write('***'+cmd+'***')

        if self._instrument_connection:
            self._instrument_connection.transport.write(cmd+SBE37Prompt.NEWLINE)
            self._instrument_connection.transport.doWrite()                
        
        prompt = yield d
        yield pu.asleep(.5)
        defer.returnValue(prompt)

    @defer.inlineCallbacks
    def _do_cmd_no_prompt(self,cmd):
        """
        Write a device command and do not await a prompt return.
        """
        
        self._debug_print('sending device command '+cmd)
       
        # Write the fragment to the IO log if it is enabled.
        if IO_LOG:
            self._logfile.write('***'+cmd+'***')

        if self._instrument_connection:
            self._instrument_connection.transport.write(cmd+SBE37Prompt.NEWLINE)
            self._instrument_connection.transport.doWrite()                
        
        yield pu.asleep(.5)


    ###########################################################################
    # Nonpublic IO helpers.
    ###########################################################################        

        
    def _parse_sample_output(self):
        """
        Parse data buffer and extract all sample output lines. Remove
        sample output lines from the data buffer, and return a list of
        samples.
        @retval A list of data sample dictionaries.
        """
        samples = []
        new_data_lines = []
        for line in self._data_lines:
            sample_data = self._sample_parser.parse(line)
            if sample_data != None:
                samples.append(sample_data)
            else:
                new_data_lines.append(line)
        self._data_lines = new_data_lines
        
        return samples


    def _read_param_values(self,lines):
        """
        Extract all parameter values from device status update output.
        Use the parameter parser objects to match and extract data line by
        line.
        """
        
        self._debug_print('reading parameter values')

        for (key,val) in self.parameters.iteritems():
            parser = val['parser']
            new_val = None
            for line in lines:
                new_val = parser.parse(line)
                if new_val != None:
                    break
            if new_val != None:
                val['value'] = new_val
            else:
                self._debug_print('could not update '+key[1])

    
    def _get_sample(self,match):
        """
        Extract sample data from a line of ascii device output matched to
        a regular expression.
        @param match a regular expression match object matching the
        expected sample data output format.
        @retval A dict containing sample data in name-value pairs:
            {'temperature':val,'conductivity':val,'pressure':val,
                'sound velocity':val,'salinity':val,'date':val,'time':val}.
            Temperature, conductivity and pressure will always be available,
            the other measurements, date and time depend on device settings.
        """

        # Extract tempreature, conductivity and pressure.
        sample_data = {}
        sample_data['temperature'] = float(match.group(1))
        sample_data['conductivity'] = float(match.group(2))
        sample_data['pressure'] = float(match.group(3))

        # Extract sound velocity and salinity if present.
        if match.group(5) and match.group(7):
            sample_data['salinity'] = float(match.group(5))
            sample_data['sound_velocity'] = float(match.group(7))
        elif match.group(5):
            if self.parameters.get(SBE37Channel.INSTRUMENT,SBE37Parameter.OUTPUTSAL)['value']:
                sample_data['salinity'] = float(match.group(5))
            elif self.parameters.get(SBE37Channel.INSTRUMENT,SBE37Parameter.OUTPUTSV)['value']:
                sample_data['sound_velocity'] = match.group(5)
        
        # Extract date and time if present.
        sample_time = None
        if  match.group(8):
            sample_time = time.strptime(match.group(8),', %d %b %Y, %H:%M:%S')
            
        elif match.group(15):
            sample_time = time.strptime(match.group(15),', %m-%d-%Y, %H:%M:%S')
        
        if sample_time:
            sample_data['device_time'] = \
                '%4i-%02i-%02iT:%02i:%02i:%02i' % sample_time[:6]

        # Add UTC time from driver in iso 8601 format.
        sample_data['driver_time'] = datetime.datetime.utcnow().isoformat()
        
        return sample_data
    
        

    
    @staticmethod
    def _date_to_string(v):
        """
        Write a date tuple to string formatted for sbe37 set operations.
        @param v a date tuple: (day,month,year).
        @retval A date string formatted for sbe37 set operations,
            or None if the input is not a valid date tuple.
        """

        if not isinstance(v,(list,tuple)):
            return None
        
        if not len(v)==3:
            return None
        
        months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep',
                  'Oct','Nov','Dec']
        day = v[0]
        month = v[1]
        year = v[2]
        
        if len(str(year)) > 2:
            year = int(str(year)[-2:])
        
        if not isinstance(day,int) or day < 1 or day > 31:
            return None
        
        if not isinstance(month,int) or month < 1 or month > 12:
            return None

        if not isinstance(year,int) or year < 0 or year > 99:
            return None
        
        return '%02i-%s-%02i' % (day,months[month-1],year)

        
    @staticmethod
    def _string_to_date(datestr,fmt):
        """
        Extract a date tuple from an sbe37 date string.
        @param str a string containing date information in sbe37 format.
        @retval a date tuple, or None if the input string is not valid.
        """
        if not isinstance(datestr,str):
            return None
        try:
            date_time = time.strptime(datestr,fmt)
            date = (date_time[2],date_time[1],date_time[0])
        except ValueError:
            return None
                        
        return date
    
    
    @staticmethod
    def _time_to_string(v):
        """
        Write a time tuple to a string formatted for sbe37 set operations.
        @param v a time tuple (hours,minutes,seconds).
        @retval A time string formatted for sbe37 set ooperations,
            or None if the input is not a valid time tuple.
        """

        if not isinstance(v,(list,tuple)):
            return None
        
        if not len(v)==3:
            return None
        
        hours = v[0]
        minutes = v[1]
        seconds = v[2]
        
        if not isinstance(hours,int) or hours < 0 or hours > 23:
            return None
        
        if not isinstance(minutes,int) or minutes < 0 or minutes > 59:
            return None

        if not isinstance(seconds,int) or seconds < 0 or seconds > 59:
            return None
        
        return '%02i%02i%02i' % (hours,minutes,seconds)


    @staticmethod
    def _true_false_to_string(v):
        """
        Write a boolean value to string formatted for sbe37 set operations.
        @param v a boolean value.
        @retval A yes/no string formatted for sbe37 set operations, or
            None if the input is not a valid bool.
        """
        
        if not isinstance(v,bool):
            return None
        if v:
            return 'y'
        else:
            return 'n'

        
    @staticmethod
    def _int_to_string(v):
        """
        Write an int value to string formatted for sbe37 set operations.
        @param v An int val.
        @retval an int string formatted for sbe37 set operations, or None if
            the input is not a valid int value.
        """
        
        if not isinstance(v,int):
            return None
        else:
            return '%i' % v

    
    @staticmethod
    def _float_to_string(v):
        """
        Write a float value to string formatted for sbe37 set operations.
        @param v A float val.
        @retval a float string formatted for sbe37 set operations, or None if
            the input is not a valid float value.
        """

        if not isinstance(v,float):
            return None
        else:
            return '%e' % v
        
    def _debug_print(self,event,data=None):
        """
        Dump state and event status to stdio.
        """
        if DEBUG_PRINT:
            print self._fsm.current_state + '  ' + event
            if isinstance(data,dict):
                for (key,val) in data.iteritems():
                    print str(key), '  ', str(val)
            elif data != None:
                print data

class SBE37DriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """
    

# Spawn of the process using the module name
factory = ProcessFactory(SBE37Driver)

