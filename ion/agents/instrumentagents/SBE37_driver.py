#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE37_driver.py
@author Edward Hunter
@brief Driver code for SeaBird SBE-37 CTD
"""

import re
import time
import datetime

from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ClientCreator

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

DEBUG_PRINT = (True,False)[0]
IO_LOG = (True,False)[1]
IO_LOG_DIR = '/Users/edwardhunter/Documents/Dev/code/logfiles/'

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
    
    
class DeviceCommandSpecification:
    """
    A translation unit of a op_* function call into a sequence of low level
    device commands for use by the appropriate state handler for writing the
    commands to the device, managing the construction of a reply and
    firing the deferred returned by the op_* call upon completion.
    """
    
    def __init__(self,command):
        self.command = command
        self.device_command_buffer = None
        self.previous_key = None
        self.errors = False
        self.deferred = None
        self.timeoutf = None
        self.reply = {'success':None,'result':{}}
        
    def do_reply(self):
        """
        Fire the command deferred with reply.
        """
        if self.timeoutf != None:
            self.timeoutf.cancel()
            self.timeoutf = None
        if self.deferred != None:
            d,self.deferred = self.deferred, None
            d.callback(self.reply)
    
    def do_timeout(self):
        """
        Fire the command deferred with timeout error.
        """
        reply = {'success':InstErrorCode.TIMEOUT,'result':{}}
        if self.deferred != None:
            d,self.deferred = self.deferred, None
            d.callback(reply)
        self.timeoutf = None
    
    def set_success(self,success_val,fail_val):
        """
        Set the overall command success message according to
        any accumulated errors.
        """
        if self.errors:
            self.reply['success'] = fail_val
        else:
            self.reply['success'] = success_val
    
    def set_previous_result(self,prev_result):
        """
        For composite device commands that set a success or other result
        with each device command response prompt, use this function to
        build up the composite result.
        """
        if self.previous_key != None:
            self.reply['result'][self.previous_key] = prev_result
    
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
        The queue of driver-level commands to be executed by the device.
        """
        self._driver_command_buffer = []  
       
        """
        The queue of instrument-level commands expanded from driver commands
        to be sent to the device.
        """
        self._device_command_buffer = []
        
        """
        The deferred that handles blocking on connect messages.
        """
        self._connection_complete_deferred = None        
        
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
            SBE37State.ACQUIRE_SAMPLE : self.state_handler_acquire_sample,
            SBE37State.UPDATE_PARAMS : self.state_handler_update_params,
            SBE37State.SET : self.state_handler_set,
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

        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)
        if event == SBE37Event.ENTER:
            # Announce the state change to agent.                        
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.UNCONFIGURED}
            self.send(self.proc_supid,'driver_event_occurred',content)
            self._initialize()

        elif event == SBE37Event.EXIT:
            pass
        
        elif event == SBE37Event.INITIALIZE:
            self._initialize()
            
        elif event == SBE37Event.CONFIGURE:
            if self._configure(params):
                next_state = SBE37State.DISCONNECTED
        
        else:
            success = InstErrorCode.INCORRECT_STATE
        return (success,next_state)
        
    
    def state_handler_disconnected(self,event,params):
        """
        Event handler for STATE_DISCONNECTED.
        Events handled:
        EVENT_ENTER: Pass.
        EVENT_EXIT: Pass.        
        EVENT_INITIALIZE: Switch to STATE_UNCONFIGURED.
        EVENT_CONNECT: Switch to STATE_CONNECTING.
        """
        
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)
        
        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.DISCONNECTED}
            self.send(self.proc_supid,'driver_event_occurred',content)
            
            # If we enter a disconnect state with the connection complete
            # defered defined, then we are entering from a previous connection
            # in response to a disconnect comment. Fire the deferred with
            # reply to indicate successful disconnect.
            if self._connection_complete_deferred:
                d,self._connection_complete_deferred = \
                    self._connection_complete_deferred,None
                reply = {'success':InstErrorCode.OK,'result':None}
                d.callback(reply)
            
        elif event == SBE37Event.EXIT:
            pass

        elif event == SBE37Event.INITIALIZE:
            next_state = SBE37State.UNCONFIGURED         
                    
        elif event == SBE37Event.CONNECT:
            next_state = SBE37State.CONNECTING         
                    
        else:
            success = InstErrorCode.INCORRECT_STATE
            
        return (success,next_state)


    def state_handler_connecting(self,event,params):
        """
        Event handler for STATE_CONNECTING.
        Events handled:
        EVENT_ENTER: Attemmpt to establish connection.
        EVENT_EXIT: Pass.        
        EVENT_CONNECTION_COMPLETE: Switch to STATE_UPDATE_PARAMS.
        EVENT_CONNECTION_FAILED: Switch to STATE_DISCONNECTED.
        """
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)
        
        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.CONNECTING}
            self.send(self.proc_supid,'driver_event_occurred',content)

            self.getConnected()
            
        elif event == SBE37Event.EXIT:
            pass
                    
        elif event == SBE37Event.CONNECTION_COMPLETE:
            next_state = SBE37State.UPDATE_PARAMS
                    
        elif event == SBE37Event.CONNECTION_FAILED:
            # Error message to agent here.
            next_state = SBE37State.DISCONNECTED

        else:
            success = InstErrorCode.INCORRECT_STATE

        return (success,next_state)


    def state_handler_disconnecting(self,event,params):
        """
        Event handler for STATE_DISCONNECTING.
        Events handled:
        EVENT_ENTER: Attempt to close connection to instrument.
        EVENT_EXIT: Pass.        
        EVENT_DISCONNECT_COMPLETE: Switch to STATE_DISCONNECTED.
        """
        
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)
            
        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.DISCONNECTED}
            self.send(self.proc_supid,'driver_event_occurred',content)            
            
            self.getDisconnected()
            
        elif event == SBE37Event.EXIT:
            pass
                    
        elif event == SBE37Event.DISCONNECT_COMPLETE:
            next_state = SBE37State.DISCONNECTED

        else:
            success = InstErrorCode.INCORRECT_STATE

        return (success,next_state)


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
        
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.CONNECTED}
            self.send(self.proc_supid,'driver_event_occurred',content)            
            
            # If we enter connected with the connection complete deferred
            # defined we are establishing the initial connection in response
            # to a connect command. Send the reply to indicate successful
            # connection.
            if self._connection_complete_deferred:
                d,self._connection_complete_deferred = \
                    self._connection_complete_deferred,None
                reply = {'success':InstErrorCode.OK,'result':None}
                d.callback(reply)
            
        elif event == SBE37Event.EXIT:
            pass
            
        elif event == SBE37Event.DISCONNECT:
            next_state = SBE37State.DISCONNECTING
                    
        elif event == SBE37Event.SET:
            next_state = SBE37State.SET
            
        elif event == SBE37Event.ACQUIRE_SAMPLE:
            next_state = SBE37State.ACQUIRE_SAMPLE
            
        elif event == SBE37Event.START_AUTOSAMPLE:
            next_state = SBE37State.AUTOSAMPLE
            
        elif event == SBE37Event.TEST:
            next_state = SBE37State.TEST
            
        elif event == SBE37Event.CALIBRATE:
            next_state = SBE37State.CALIBRATE
            
        elif event == SBE37Event.RESET:
            next_state = SBE37State.RESET
            
        elif event == SBE37Event.DATA_RECEIVED:
            pass
                
        else:
            success = InstErrorCode.INCORRECT_STATE

        return (success,next_state)

        
    def state_handler_acquire_sample(self,event,params):
        """
        Event handler for STATE_ACQUIRE_SAMPLE.
        Events handled:
        EVENT_ENTER: Initialize command buffers and start looping wakeup.
        EVENT_EXIT: Clear buffers and send reply.        
        EVENT_PROMPTED: Cancel wakeup, write device commands, parse output
        and populate reply. Switch to STATE_CONNECTED.
        EVENT_DATA_RECEIVED: Pass.
        """
        
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == SBE37Event.ENTER:
                        
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.ACQUIRE_SAMPLE}
            self.send(self.proc_supid,'driver_event_occurred',content)                                    
                        
            # Initialize data buffer and copy device command buffer over
            # from the acquire command spec.
            command_spec = self._driver_command_buffer[0]
            self._data_lines = []
            self._sample_buffer = []
            self._device_command_buffer = command_spec.device_command_buffer
                        
            # Start the looping wakeup.
            self._start_wakeup(2.0)
            
        elif event == SBE37Event.EXIT:

            # Clear the data buffer.
            self._data_lines = []
            #self._device_command_buffer = []
            
            # Pop the command spec, set success and fire the reply deferred.
            command_spec = self._driver_command_buffer.pop(0)
            if len(self._sample_buffer)==0:
                command_spec.errors = True
            elif len(self._sample_buffer)==1:
                command_spec.reply['result'] = self._sample_buffer[0]
            else:
                command_spec.reply['result'] = self._sample_buffer
                
            command_spec.set_success(InstErrorCode.OK,
                                     InstErrorCode.ACQUIRE_SAMPLE_ERR)
            command_spec.do_reply()
            self._sample_buffer = []        
                    
                    
        elif event == SBE37Event.PROMPTED:
                        
                    
            # Cancel looping wakeup if active.
            self._stop_wakeup()
                
            # Pop the device command.
            try:
                
                cmd = self._device_command_buffer.pop(0)
            
            # If no further commands, parse output, set result and switch
            # state.
            except IndexError:
                    
                next_state = SBE37State.CONNECTED

            # Write the device command.
            else:
                
                self._write_command(cmd+SBE37Prompt.NEWLINE)
                                
        elif event == SBE37Event.DATA_RECEIVED:

            #content = {'type':DriverAnnouncement.STATE_CHANGE,
            # 'transducer':SBE37Channel.INSTRUMENT,
            #           'value':SBE37State.UNCONFIGURED}
            #self.send(self.proc_supid,'driver_event_occurred',content)
        

            # Parse the data buffer for sample output. Publish these as
            # appropriate, and package them in the reply if the option
            # has been set.
            samples = self._parse_sample_output()
            if len(samples)>0:
                self._sample_buffer += samples
                if len(samples)==1:
                    samples = samples[0]                
                self._debug_print('received samples',samples)
                content = {'type':DriverAnnouncement.DATA_RECEIVED,
                           'transducer':SBE37Channel.INSTRUMENT,'value':samples}
                self.send(self.proc_supid,'driver_event_occurred',content)                                                
            
        else:
            success = InstErrorCode.INCORRECT_STATE
        
        return (success,next_state)


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
        
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == SBE37Event.ENTER:

            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.AUTOSAMPLE}
            self.send(self.proc_supid,'driver_event_occurred',content)                                    

            # Clear the data buffer and copy the device command buffer
            # from the autosample command spec.
            command_spec = self._driver_command_buffer[0]
            self._data_lines = []
            self._sample_buffer = []
            self._device_command_buffer = command_spec.device_command_buffer
                        
            # Start the looping wakeup.
            self._start_wakeup(2.0)

        elif event == SBE37Event.EXIT:

            # Pop the command spec.                
            command_spec = self._driver_command_buffer.pop(0)

            # If this is a stop command with getdata arg, populate the reply.
            drv_cmd = command_spec.command
            if list(drv_cmd) == [SBE37Command.STOP_AUTO_SAMPLING,'GETDATA']:
                if len(self._sample_buffer)==0:
                    command_spec.errors = True
                elif len(self._sample_buffer)==1:
                    command_spec.reply['result'] = self._sample_buffer[0]
                else:
                    command_spec.reply['result'] = self._sample_buffer

            # Clear the data buffer, set success and fire reply deferred.
            self._data_lines = []
            self._sample_buffer = []
            command_spec.set_success(InstErrorCode.OK,
                                     InstErrorCode.ACQUIRE_SAMPLE_ERR)
            command_spec.do_reply()
                                    
        elif event == SBE37Event.PROMPTED:
            
            # Cancel looping wakeup if active.
            self._stop_wakeup()

            # Get the driver command.                
            command_spec = self._driver_command_buffer[0]
            drv_cmd = command_spec.command

            if drv_cmd[0] == SBE37Command.START_AUTO_SAMPLING:

                # Pop the pending autosample device command.
                try:
                    cmd = self._device_command_buffer.pop(0)

                except IndexError:
                    pass

                # Write autosample device command, remove command spec
                # from buffer, set result and fire the reply deferred.
                else:
                    self._write_command(cmd+SBE37Prompt.NEWLINE)
                    self._driver_command_buffer.pop(0)
                    command_spec.set_success(InstErrorCode.OK,
                                             InstErrorCode.ACQUIRE_SAMPLE_ERR)
                    command_spec.do_reply()
                    
            elif drv_cmd[0] == SBE37Command.STOP_AUTO_SAMPLING:

                # Pop pending autosample stop device command.
                try:
                    cmd = self._device_command_buffer.pop(0)
                
                # If no further autosample stop device commands, send wakeup
                # until we get the correct prompt to ensure we are in the
                # right state.
                except IndexError:
                    if self._line_buffer == '':
                        self._wakeup()
                    else:
                        next_state = SBE37State.CONNECTED

                # Write the autosample stop device command.                
                else:
                    self._write_command(cmd+SBE37Prompt.NEWLINE)
                    
        elif event == SBE37Event.STOP_AUTOSAMPLE:

            # Do not clear data buffer, copy over the stop autosample
            # device command buffer.
            command_spec = self._driver_command_buffer[0]
            self._device_command_buffer = command_spec.device_command_buffer

            # Start the looping wakeup.
            self._start_wakeup(2.0)
            
        elif event == SBE37Event.DATA_RECEIVED:
            
            # Parse the data buffer for sample output. Publish these as
            # appropriate, and package them in the reply if the option
            # has been set.
            samples = self._parse_sample_output()
            if len(samples)>0:
                self._sample_buffer += samples
                if len(samples)==1:
                    samples = samples[0]
                self._debug_print('received samples',samples)
                content = {'type':DriverAnnouncement.DATA_RECEIVED,
                           'transducer':SBE37Channel.INSTRUMENT,'value':samples}
                self.send(self.proc_supid,'driver_event_occurred',content)                                                
            
        else:
            
            success = InstErrorCode.INCORRECT_STATE
        
        return (success,next_state)           


    def state_handler_update_params(self,event,params):
        """
        Event handler for STATE_UPDATE_PARAMS.
        EVENT_ENTER: Init command variables, populate device command buffer
        and send wakeup.
        EVENT_EXIT: Clear command variables.        
        EVENT_PROMPTED: Write next command to device. If done, read param
        values from data buffer and switch to STATE_CONNECTED.
        EVENT_DATA_RECEIVED: Pass.
        """
        
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.UPDATE_PARAMS}
            self.send(self.proc_supid,'driver_event_occurred',content)                                    
              
            # Clear data buffer and populate device command buffer directly.
            self._data_lines = []
            self._device_command_buffer = ['DS','DC']
           
            # Start looping wakeup.
            self._start_wakeup(2.0)
            
        elif event == SBE37Event.EXIT:
            
            # Clear data buffer.
            self._data_lines = []

            # Pop the current command spec. If it exists, it triggered
            # this update.
            try:
                
                command_spec = self._driver_command_buffer.pop(0)
            
            # No command spec, update triggered internally. No action
            # required.
            except IndexError:
                
                pass
            
            # Current command spec exists, fire reply deferred.
            else:
                
                command_spec.do_reply()
                    
            # Announce the config change to agent. This assumes
            # that param updates occur one-to-one with config changes.
            paramdict = self.get_parameter_dict()
            content = {'type':DriverAnnouncement.CONFIG_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':paramdict}
            self.send(self.proc_supid,'driver_event_occurred',content)                                                
                    
        elif event == SBE37Event.PROMPTED:
            
            # Cancel the looping wakeup if active.
            self._stop_wakeup()

            if self._line_buffer == '':
                self._debug_print('detected empty line buffer')
                self._wakeup()
                
            else:
            
                # Pop the next pending device command.
                try:
                    
                    cmd = self._device_command_buffer.pop(0)
    
                # If no further device command, parse the device output and
                # switch state.
                except IndexError:
                    
                    self._read_param_values(self._data_lines)
                    next_state = SBE37State.CONNECTED
    
                # Write command to device.
                else:
                    self._write_command(cmd+SBE37Prompt.NEWLINE)
                
        elif event == SBE37Event.DATA_RECEIVED:
            pass
                
        else:
            success = InstErrorCode.INCORRECT_STATE

        return (success,next_state)


    def state_handler_set(self,event,params):
        """
        Event handler for STATE_SET.
        EVENT_ENTER: Init command variables, populate device command buffer
        and start looping wakeup.
        EVENT_EXIT: Clear command and data buffers. Reply is sent as part of
        update params state.
        EVENT_PROMPTED: Write next set command to device. Validate previous
        set command with data buffer. Switch to STATE_UPDATE_PARAMS if done.
        EVENT_DATA_RECEIVED: Pass.
        """
        
        success = InstErrorCode.OK
        next_state = None
        self._debug_print(event)

        if event == SBE37Event.ENTER:
            
            # Announce the state change to agent.            
            content = {'type':DriverAnnouncement.STATE_CHANGE,
                       'transducer':SBE37Channel.INSTRUMENT,
                       'value':SBE37State.SET}
            self.send(self.proc_supid,'driver_event_occurred',content)                                                
            
            # Clear the data buffer and copy the device command buffer over
            # from the command spec.
            command_spec = self._driver_command_buffer[0]
            self._data_lines = []
            self._device_command_buffer = command_spec.device_command_buffer
                            
            # Start the looping wakeup.
            self._start_wakeup(2.0)

        elif event == SBE37Event.EXIT:
            
            # Clear the data buffer.
            self._data_lines = []
                    
        elif event == SBE37Event.PROMPTED:
            
            #print 'line buf: '+self._line_buffer+'xx'
            # Cancel the looping wakeup if active.
            self._stop_wakeup()
            
            # Validate previous response.
            command_spec = self._driver_command_buffer[0]
            if self._line_buffer == SBE37Prompt.PROMPT:
                prev_result = InstErrorCode.OK
            else:
                prev_result = InstErrorCode.BAD_DRIVER_COMMAND
            command_spec.set_previous_result(prev_result)
            
            # Pop the pending device set command.
            try:
                
                (set_key,set_val,set_cmd) = self._device_command_buffer.pop(0)

            # If no remainind device set commands, set success and switch
            # state.
            except IndexError:
                
                command_spec.set_success(InstErrorCode.OK,
                                         InstErrorCode.SET_DEVICE_ERR)
                next_state = SBE37State.UPDATE_PARAMS

            # Write the device set command.                
            else:
                
                command_spec.previous_key = set_key
                #print 'writing command '+set_cmd
                self._write_command(set_cmd+SBE37Prompt.NEWLINE)

        elif event == SBE37Event.DATA_RECEIVED:
            pass                
                        
        else:
            success = InstErrorCode.INCORRECT_STATE
            
        return (success,next_state)


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
    def getConnected(self):
        """
        Called by this class to establish connection with the device.
        Send EVENT_CONNECTION_COMPLETE if successful or
        EVENT_CONNECTION_FAILED if not successful.
        """
        
        cc = ClientCreator(reactor, InstrumentConnection, self)
        self._instrument_connection = yield cc.connectTCP(self._ipaddr,
                                                          int(self._ipport))        
        
        
        if self._instrument_connection:
            self._instrument_connection.transport.setTcpNoDelay(1)
            self._fsm.on_event(SBE37Event.CONNECTION_COMPLETE)
        else:
            self._fsm.on_event(SBE37Event.CONNECTION_FAILED)


    def gotConnected(self, instrument):
        """
        Called from twisted framework when connection is established.
        """
        pass

    def getDisconnected(self):
        """
        Called by this class to close the connection to the device.
        """
        if self._instrument_connection:
            self._instrument_connection.transport.loseConnection()
         
 
    def gotDisconnected(self, instrument):
        """
        Called by the twisted framework when the connection is closed.
        Send an EVENT_DISCONNECT_COMPLETE.
        """

        self._instrument_connection = None
        self._fsm.on_event(SBE37Event.DISCONNECT_COMPLETE)


    def gotData(self, dataFrag):
        """
        Called by the twisted framework when a data fragment is received.
        Update line and data buffers. Send EVENT_DATA_RECEIVED and
        EVENT_PROMPTED if a prompt is detected.
        @param dataFrag a string data fragment received.
        """
        
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
        if new_lines:
            self._fsm.on_event(SBE37Event.DATA_RECEIVED)
        
        # If a normal or bad command prompt is detected, send an
        # EVENT_PROMPTED
        if (self._line_buffer == SBE37Prompt.PROMPT or
            self._line_buffer == SBE37Prompt.BAD_COMMAND):
            self._fsm.on_event(SBE37Event.PROMPTED)
            
        # If a stop autosample type prompt is detected, send an
        # EVENT_PROMPTED.
        elif (self._line_buffer == '' and
              (len(self._data_lines)>0 and self._data_lines[-1] == SBE37Prompt.PROMPT)):
            self._fsm.on_event(SBE37Event.PROMPTED)


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

            # Create command spec and set event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = SBE37Event.ACQUIRE_SAMPLE
            
            # The acquire command only applies to the instrument as a whole.
            # Fail if the channel is not set properly.
            if len(channels)>1 or channels[0] != SBE37Channel.INSTRUMENT:
                reply['success'] = InstErrorCode.INVALID_CHANNEL
                yield self.reply_ok(msg,reply)
                return

            # The default device command is TS, append this to device
            # command buffer if no optional argument is present.
            if len(command)==1:
                command_spec.device_command_buffer = ['TS']
                
            # If the optional argument is present, check that it is valid
            # for sbe37. If so append it to device command buffer.
            elif len(command)==2:                
                if command[1] not in sbe37_cmd_acquire_sample_args:
                    reply['success'] = InstErrorCode.INVALID_COMMAND
                    yield self.reply_ok(msg,reply)
                    return
                else:
                    command_spec.device_command_buffer = command[1]
            
            # If the command has a length greater than 2, fail.      
            else:
                reply['success'] = InstErrorCode.INVALID_COMMAND
                yield self.reply_ok(msg,reply)
                return

            # Set up the reply deferred and fire the command event.  
            reply = yield self._process_command(command_spec,event,timeout)

        # Process start autosampling command.
        elif drv_cmd == SBE37Command.START_AUTO_SAMPLING:

            # Create command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = SBE37Event.START_AUTOSAMPLE

            # The acquire command only applies to the instrument as a whole.
            # Fail if channel not set properly.
            if len(channels)>1 or channels[0] != SBE37Channel.INSTRUMENT:
                reply['success'] = InstErrorCode.INVALID_CHANNEL
                yield self.reply_ok(msg,reply)
                return

            # Append the sbe37 start command to the device command buffer.                                
            command_spec.device_command_buffer = ['STARTNOW']                

            # Set up the reply deferred and fire the command event.  
            reply = yield self._process_command(command_spec,event,timeout)

        # Process stop autosampling command.
        elif drv_cmd == SBE37Command.STOP_AUTO_SAMPLING:
            
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = SBE37Event.STOP_AUTOSAMPLE

            # The acquire command only applies to the instrument as a whole.
            # Fail if channel not set properly.
            if len(channels)>1 or channels[0] != SBE37Channel.INSTRUMENT:
                reply['success'] = InstErrorCode.INVALID_CHANNEL
                yield self.reply_ok(msg,reply)
                return
            
            # The command has one optional argument,
            # that retrieves the autosampled data in the stop reply.
            if len(command)>2:
                reply['success'] = InstErrorCode.INVALID_COMMAND
                yield self.reply_ok(msg,reply)
                return

            if len(command)==2 and command[1]!='GETDATA':
                reply['success'] = InstErrorCode.INVALID_COMMAND
                yield self.reply_ok(msg,reply)
                return

            # Append the sbe37 command to the device command buffer.                                
            command_spec.device_command_buffer = ['STOP']                

            # Set up the reply deferred and fire the command event.  
            reply = yield self._process_command(command_spec,event,timeout)

        # Process test command.
        elif drv_cmd == SBE37Command.TEST:
            
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = SBE37Event.TEST  
            
            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg,reply)
            return

        elif drv_cmd == SBE37Command.CALIBRATE:
            
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = SBE37Event.CALIBRATE
            
            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg,reply)
            return

        elif drv_cmd == SBE37Command.RESET:
                       
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = SBE37Event.RESET

            # Return not implemented reply.
            reply['success'] = InstErrorCode.NOT_IMPLEMENTED
            yield self.reply_ok(msg,reply)
            return

        elif drv_cmd == SBE37Command.TEST_ERRORS:
                       
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            
            #reply = {'success':InstErrorCode.OK,'result':{}}
            
            # This raises an internal server error on the client side with
            # the reply contained in the exception content.
            #yield self.reply_err(msg,reply)
            
            # These raise internal server errors on the client side, but
            # the exception type or error message content is unknown.
            #raise DriverException('Error!')
            #raise defer.TimeoutError()
            
            # These don't serialize.
            #yield self.reply_err(msg,DriverException('Error!'))
            #yield self.reply_err(msg,defer.TimeoutError())
            #return

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
        
        # Retrieve the requested parameters from the driver parameter
        # variables and send the reply. 
        reply = self._get_parameters(params)
        
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
        
        # Create the command spec and set the event to fire.
        command_spec = DeviceCommandSpecification([SBE37Command.SET])        
        event = SBE37Event.SET
        
        # Populate the device command buffer with the equivalent
        # sbe37 set commands.
        device_command_buffer = []
        sample_num_command = None
        for (chan,param) in params.keys():
            val = params[(chan,param)]
            if self.parameters.get((chan,param),None):
                str_val = self.parameters[(chan,param)]['parser'].tostring(val)
                if str_val:
                    if (chan,param) == (SBE37Channel.INSTRUMENT,'SAMPLENUM'):
                        sample_num_command = ((SBE37Channel.INSTRUMENT,
                                              'SAMPLENUM'),val,param+'='+str_val)
                    else:
                        device_command_buffer.append(((chan,param),val,
                            param+'='+str_val))
                else:
                    # Parser error creating value string.
                    pass
            else:
                command_spec.reply['result'][(chan,param)] = \
                    InstErrorCode.INVALID_PARAMETER
                command_spec.errors = True

        # Move set sample number, if it exists to the end as other commands
        # can reset this to zero.
        if sample_num_command != None:
            device_command_buffer.append(sample_num_command)

        # If no valid commands, reply error.
        if len(device_command_buffer)==0:
            reply = command_spec.reply
            reply['success'] = InstErrorCode.SET_DEVICE_ERR
            yield self.reply_ok(msg,reply)
            return
            
        # Otherwise, set device command buffer and process command.
        else:
            command_spec.device_command_buffer = device_command_buffer
            reply = yield self._process_command(command_spec,event)

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
        success = self._fsm.on_event(SBE37Event.INITIALIZE)
        
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
        reply['success'] = self._validate_configuration(params)

        if InstErrorCode.is_error(reply['success']):
            yield self.reply_ok(msg,reply)
            return
        
        # Fire EVENT_CONFIGURE with the validated configuration parameters.
        # Set the error message if the event is not handled in the current
        # state.
        reply['success'] = self._fsm.on_event(SBE37Event.CONFIGURE,params)

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

        # Create the connection complete deferred and fire EVENT_CONNECT.
        reply = yield self._process_connect() 
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

        # Create the connection complete deferred and fire EVENT_DISCONNECT.
        reply = yield self._process_disconnect()
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
    # Nonpublic methods.
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
            return False

        # Set configuration parameters.
        self._ipport = params['ipport']
        self._ipaddr = params['ipaddr']

        return True


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
        

    def _process_connect(self):
        """
        Process a connect command.
        @retval A deferred that will fire with a reply message on completion
        of the connect processing. The reply message is a dict
        {'success':success,'result':None}
        """
        
        # Create the connection complete deferred to be fired with a reply
        # at the conclusion of the command.
        d = defer.Deferred()        
        self._connection_complete_deferred = d
        
        # Fire EVENT_CONNECT. If the event fails the state is wrong.
        success = self._fsm.on_event(SBE37Event.CONNECT)
        if InstErrorCode.is_error(success):
            reply = {'success':success,'result':None}
            d,self._connection_complete_deferred = self._connection_complete_deferred,None
            d.callback(reply)            

        return d


    def _process_disconnect(self):
        """
        Process a disconnect command.
        @retval A deferred that will fire with a reply message on completion
        of the disconnect command. The reply message is a dict
        {'success':success,'result':None}
        """
        
        # Create the connection complete deferred to be fired with a reply
        # at the conclusion of the command.
        d = defer.Deferred()        
        self._connection_complete_deferred = d
        
        # Fire EVENT_DISCONNECT. If the event fails the state is wrong.
        success = self._fsm.on_event(SBE37Event.DISCONNECT)
        #if not success:
        if InstErrorCode.is_error(success):
            reply = {'success':success,'result':None}
            d,self._connection_complete_deferred = self._connection_complete_deferred,None
            d.callback(reply)            

        return d
        
        
    def _process_command(self,command_spec,event,timeout=0):
        """
        Process a device command.
        Append the command to the driver command buffer and fire the
        event associated with the arrival of the command.
        @param command_spec a DeviceCommandSpecification that describes
            the command to execute.
        @param event the event to handle the command.
        @param timeout the driver timeout after which the driver will
            cancel the command and return a timeout error.
        @retval A deferred that will be fired upon conclusion of the
        command processing with a command specific reply message.
        """
        
        
        # Create the command deferred to be returned.
        d = defer.Deferred()        
        command_spec.deferred = d
                            
        # Add the command specification to the driver command buffer.
        self._driver_command_buffer.append(command_spec)                
        
        # If a timeout is specified, define and position it for callback.
        if timeout > 0:

            def _timeoutf(cs,driver):
                driver._debug_print('in timeout func for command '+ str(cs.command))
    
                # If this command is still active apply timeout ops.
                # If this doesn't match, then the operation completed normally
                # and was already popped from the buffer.
                if len(self._driver_command_buffer) > 0:
                    if cs == self._driver_command_buffer[0]:
        
                        # Pop the timed out command from the buffer.
                        driver._debug_print('popping command')
                        self._driver_command_buffer.pop(0)
        
                        # Cancel any looping wakeup.
                        self._stop_wakeup()
                        
                        # Fire the reply deferred with timeout error.
                        driver._debug_print('firing timeout')
                        cs.do_timeout()
        
            command_spec.timeoutf = reactor.callLater(timeout,_timeoutf,
                                                     command_spec,self)                    
        
        # Fire the command received event and return deferred.
        if self._fsm.on_event(event):
            return d
        else:
            reply = {'success':InstErrorCode.INCORRECT_STATE,'result':None}
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
        reply = {'success':None,'result':None}
        result = {}
        get_errors = False

        for (chan,arg) in params:
            if SBE37Channel.has(chan) and SBE37Status.has(arg):
                # If instrument channel or all.
                if chan == SBE37Channel.INSTRUMENT or chan == SBE37Channel.ALL:
                    if arg == SBE37Status.DRIVER_STATE or arg == SBE37Status.ALL:
                        result[(SBE37Channel.INSTRUMENT,SBE37Status.DRIVER_STATE)] = \
                            (InstErrorCode.OK,self._fsm.get_current_state())
                    
                    if arg == SBE37Status.OBSERVATORY_STATE or arg == SBE37Status.ALL:
                        result[(SBE37Channel.INSTRUMENT,SBE37Status.OBSERVATORY_STATE)] = \
                            (InstErrorCode.OK,self._get_observatory_state())
                    
                    if arg == SBE37Status.DRIVER_ALARMS or arg == SBE37Status.ALL:
                        result[(SBE37Channel.INSTRUMENT,SBE37Status.DRIVER_ALARMS)] = \
                            (InstErrorCode.OK,self._alarms)
                    
                    if arg == SBE37Status.DRIVER_VERSION or arg == SBE37Status.ALL:
                        result[(SBE37Channel.INSTRUMENT,SBE37Status.DRIVER_VERSION)] = \
                            (InstErrorCode.OK,self.get_version())
    
                # If conductivity channel or all.            
                if chan == SBE37Channel.CONDUCTIVITY or chan == SBE37Channel.ALL:
                    if arg == SBE37Status.ALL:
                        pass
                        
                    # Conductivity channel does not support this status.
                    else:
                        result[(chan,arg)] = (InstErrorCode.INVALID_STATUS,None)
                        get_errors = True
                        
                if chan == SBE37Channel.PRESSURE or chan == SBE37Channel.ALL:
                    if arg == SBE37Status.ALL:
                        pass

                    # Pressure channel does not support this status.                        
                    else:
                        result[(chan,arg)] = (InstErrorCode.INVALID_STATUS,None)
                        get_errors = True
    
                # If pressure channel or all.
                if chan == SBE37Channel.TEMPERATURE or chan == SBE37Channel.ALL:
                    if arg == SBE37Status.ALL:
                        pass
                        
                    # Temp channel does not support this status.
                    else:
                        result[(chan,arg)] = (InstErrorCode.INVALID_STATUS,None)
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


    def get_parameter_dict(self):
        """
        Return a dict with all driver paramters.
        """
        paramdict = dict(map(lambda x: (x[0],x[1]['value']),self.parameters.items()))
        return paramdict


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

        
    def _write_command(self,cmd):
        """
        Write a command to the device.
        """
        # Write the fragment to the IO log if it is enabled.
        if IO_LOG:
            self._logfile.write('***'+cmd+'***')

        if self._instrument_connection:
            self._instrument_connection.transport.write(cmd)
            self._instrument_connection.transport.doWrite()                

        
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
            #sample_data['date'] = (sample_time[2],sample_time[1],sample_time[0])
            #sample_data['time'] = (sample_time[3],sample_time[4],sample_time[5])            
            #sample_data['date'] = '%i-%i-%i' % (sample_time[1],sample_time[2],sample_time[0])
            #sample_data['time'] = '%i:%i:%i' % (sample_time[3],sample_time[4],sample_time[5]) 
            sample_data['device_time'] = \
                '%4i-%02i-%02iT:%02i:%02i:%02i' % sample_time[:6]
            print sample_time

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
        
        if not isinstance(day,int) or day < 1 or day > 31:
            return None
        
        if not isinstance(month,int) or month < 1 or month > 12:
            return None

        if not isinstance(year,int) or year < 0 or year > 99:
            return None
        
        return '%02i-%s-%02i' % (day,cls.months[month],year)

        
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
    def _time_to_sting(v):
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

