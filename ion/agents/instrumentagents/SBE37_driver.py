#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE37_driver.py
@author Edward Hunter
@brief Driver code for SeaBird SBE-37 CTD
"""


import re
import time

from twisted.internet import defer, reactor, task
from twisted.internet.protocol import ClientCreator
import ion.util.ionlog
import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.agents.instrumentagents.instrument_connection import InstrumentConnection

from ion.agents.instrumentagents.instrument_agent import InstrumentDriver
from ion.agents.instrumentagents.instrument_agent import InstrumentDriverClient, errors
from ion.agents.instrumentagents.ion_fsm import IonFiniteStateMachine
from ion.agents.instrumentagents.instrument_agent_constants import driver_command_list
from ion.agents.instrumentagents.instrument_agent_constants import driver_state_list
from ion.agents.instrumentagents.instrument_agent_constants import driver_event_list
from ion.core.exception import ApplicationError

log = ion.util.ionlog.getLogger(__name__)

DEBUG_PRINT = (True,False)[0]
IO_LOG = (True,False)[1]
IO_LOG_DIR = '/Users/edwardhunter/Documents/Dev/code/logfiles/'

###############################################################################
# Constants specific to the SBE37Driver. 
###############################################################################


"""
Device prompts.
"""
SBE37_PROMPT = 'S>'
SBE37_NEWLINE = '\r\n'
SBE37_BAD_COMMAND = '?cmd S>'

"""
Device states. Add SBE37 specific states here.
"""
sbe37_state_list = driver_state_list

"""
Device events. Add SBE37 specific events here.
"""
sbe37_event_list = driver_event_list

"""
Device commands. Add SBE37 specific commands here.
"""
sbe37_command_list = driver_command_list

"""
Device channels. A list of driver_channel_list elements specific to this
instrument.
"""
sbe37_channel_list = [
    'CHAN_INSTRUMENT',
    'CHAN_TEMPERATURE',
    'CHAN_PRESSURE',
    'CHAN_CONDUCTIVITY'
]

###############################################################################
# To do. Use of these constants needs design.
###############################################################################

"""
Command acquire sample arguments.
"""
sbe37_cmd_acquire_sample_args = [
    'TS',
    'TSR',
    'TSS',
    'TSSON',
    'SLT',
    'SLTR',
    'SL'
]
     
"""
Command test arguments. 
"""
sbe37_cmd_test_args = [
    'TT',
    'TC',
    'TP',
    'TTR',
    'TCR',
    'TPR',
    'TR'
]


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
        reply = {'success':errors['TIMEOUT'],'result':{}}
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
        Dictionary of instrument parameters. Each parameter object knows
        how to match and read itself from a string, and write itself in a
        string format the device will accept.
        """
        self.parameters = {
            # This needs to be fixed and should be added to status, metadata or
            # somewhere else. Also need a set clock function.
            #('CHAN_INSTRUMENT','DDMMYY'):{
            #    'value': None,
            #    'parser': DeviceIOParser(
            #        r'SBE[\w-]* +V +[\w.]+ + SERIAL +NO. +\d+ +(\d+) +([a-zA-Z]+) +(\d+) +\d+:\d+:\d+',
            #        lambda match: (int(match.group(1)),self.month2int(match.group(1)),int(match.group(3))),
            #        self.date2str_mmddyy)
            #    },
            #('CHAN_INSTRUMENT','HHMMSS'):{
            #    'value': None,
            #    'parser': DeviceIOParser(
            #        r'SBE[\w-]* +V +[\w.]+ + SERIAL +NO. +\d+ +\d+ +[a-zA-Z]+ +\d+ +(\d+):(\d+):(\d+)',
            #        lambda match: ( int(match.group(1)),self.month2int(match.group(2)),int(match.group(3))),
            #        self._time_to_string)
            #    },
            # TBD how does this get handled?
            #('CHAN_INSTRUMENT','BAUD'):{
            #    'value': None,
            #    'parser': None,
            #    'default':'9600'
            #    },
            ('CHAN_INSTRUMENT','OUTPUTSAL'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'(do not )?output salinity with each sample',
                    lambda match : False if match.group(1) else True,
                    self._true_false_to_string)
                },
            ('CHAN_INSTRUMENT','OUTPUTSV'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'(do not )?output sound velocity with each sample',
                    lambda match : False if match.group(1) else True,
                    self._true_false_to_string)
                },
            # TBD how does this get handled?
            #('CHAN_INSTRUMENT','FORMAT'):{
            #    'value': None,
            #    'parser': DeviceIOParser(
            #        r'',
            #        lambda match : 0 ),
            #    'default':1
            #    },
            ('CHAN_INSTRUMENT','NAVG'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'number of samples to average = (\d+)',
                    lambda match : int(match.group(1)),
                    self._int_to_string)       
                },
            ('CHAN_INSTRUMENT','SAMPLENUM'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'samplenumber = (\d+), free = \d+',
                    lambda match : int(match.group(1)),
                    self._int_to_string)       
                },
            ('CHAN_INSTRUMENT','INTERVAL'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'sample interval = (\d+) seconds',
                    lambda match : int(match.group(1)),
                    self._int_to_string)       
                 },
            ('CHAN_INSTRUMENT','STORETIME'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'(do not )?store time with each sample',
                    lambda match : False if match.group(1) else True,
                    self._true_false_to_string)
                },
            ('CHAN_INSTRUMENT','TXREALTIME'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'(do not )?transmit real-time data',
                    lambda match : False if match.group(1) else True,
                    self._true_false_to_string)
                },
            ('CHAN_INSTRUMENT','SYNCMODE'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'serial sync mode (enabled|disabled)',
                    lambda match : False if (match.group(1)=='disabled') else True,
                    self._true_false_to_string)
                },
            ('CHAN_INSTRUMENT','SYNCWAIT'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'wait time after serial sync sampling = (\d+) seconds',
                    lambda match : int(match.group(1)),
                    self._int_to_string)
                },
            # Should cal dates be parameters or metadata?
            ('CHAN_TEMPERATURE','TCALDATE'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'temperature: +((\d+)-([a-zA-Z]+)-(\d+))',
                    lambda match : self._string_to_date(match.group(1),
                                                        '%d-%b-%y'),
                    self._date_to_string)
                },
            ('CHAN_TEMPERATURE','TA0'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +TA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_TEMPERATURE','TA1'):{
                'value': None,
                'parser': DeviceIOParser(
                   r' +TA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_TEMPERATURE','TA2'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +TA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_TEMPERATURE','TA3'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +TA3 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            # Should cal dates be parameters or metadata?                
            ('CHAN_CONDUCTIVITY','CCALDATE'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'conductivity: +((\d+)-([a-zA-Z]+)-(\d+))',
                    lambda match : self._string_to_date(match.group(1),
                                                        '%d-%b-%y'),
                    self._date_to_string)
                },
            ('CHAN_CONDUCTIVITY','CG'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +G = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_CONDUCTIVITY','CH'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +H = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_CONDUCTIVITY','CI'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +I = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_CONDUCTIVITY','CJ'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +J = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_CONDUCTIVITY','WBOTC'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +WBOTC = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_CONDUCTIVITY','CTCOR'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +CTCOR = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_CONDUCTIVITY','CPCOR'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +CPCOR = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            # Should cal dates be parameters or metadata?                
            ('CHAN_PRESSURE','PCALDATE'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'pressure .+ ((\d+)-([a-zA-Z]+)-(\d+))',
                    lambda match : self._string_to_date(match.group(1),
                                                        '%d-%b-%y'),
                    self._date_to_string)
                },
            ('CHAN_PRESSURE','PA0'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','PA1'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','PA2'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','PTCA0'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','PTCA1'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','PTCA2'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','PTCB0'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCSB0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','PTCB1'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCSB1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','PTCB2'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +PTCSB2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_PRESSURE','POFFSET'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +POFFSET = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            # Should cal dates be parameters or metadata?                
            ('CHAN_INSTRUMENT','RCALDATE'):{
                'value': None,
                'parser': DeviceIOParser(
                    r'rtc: +((\d+)-([a-zA-Z]+)-(\d+))',
                    lambda match : self._string_to_date(match.group(1),
                                                        '%d-%b-%y'),
                    self._date_to_string)
                },
            ('CHAN_INSTRUMENT','RTCA0'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +RTCA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_INSTRUMENT','RTCA1'):{
                'value': None,
                'parser': DeviceIOParser(
                    r' +RTCA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                    lambda match : float(match.group(1)),
                    self._float_to_string)
                },
            ('CHAN_INSTRUMENT','RTCA2'):{
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
        self.state_handlers = {
            'STATE_UNCONFIGURED':self.state_handler_unconfigured,
            'STATE_DISCONNECTED':self.state_handler_disconnected,
            'STATE_CONNECTING':self.state_handler_connecting,
            'STATE_DISCONNECTING':self.state_handler_disconnecting,
            'STATE_CONNECTED':self.state_handler_connected,
            'STATE_ACQUIRE_SAMPLE':self.state_handler_acquire_sample,
            'STATE_UPDATE_PARAMS':self.state_handler_update_params,
            'STATE_SET':self.state_handler_set,
            'STATE_AUTOSAMPLE':self.state_handler_autosample
        }
        
        """
        Instrument state machine.
        """
        self.fsm = IonFiniteStateMachine(sbe37_state_list,
                                         sbe37_event_list,
                                         self.state_handlers)


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
        
        success = True
        next_state = None
        self._debug_print(event)
        
        if event == 'EVENT_ENTER':
            
            # Announce the state change to agent.                        
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_UNCONFIGURED'}
            self.send(self.proc_supid,'driver_event_occurred',content)
            
            self._initialize()

        elif event == 'EVENT_EXIT':
            pass
        
        elif event == 'EVENT_INITIALIZE':
            self._initialize()
            
        elif event == 'EVENT_CONFIGURE':
            if self._configure(params):
                next_state = 'STATE_DISCONNECTED'
        
        else:
            success = False
        
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
        
        success = True
        next_state = None
        self._debug_print(event)
        
        if event == 'EVENT_ENTER':
            
            # Announce the state change to agent.            
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_DISCONNECTED'}
            self.send(self.proc_supid,'driver_event_occurred',content)
            
            # If we enter a disconnect state with the connection complete
            # defered defined, then we are entering from a previous connection
            # in response to a disconnect comment. Fire the deferred with
            # reply to indicate successful disconnect.
            if self._connection_complete_deferred:
                d,self._connection_complete_deferred = self._connection_complete_deferred,None
                reply = {'success':['OK'],'result':None}
                d.callback(reply)
            
        elif event == 'EVENT_EXIT':
            pass

        elif event == 'EVENT_INITIALIZE':
            next_state = 'STATE_UNCONFIGURED'         
                    
        elif event == 'EVENT_CONNECT':
            next_state = 'STATE_CONNECTING'         
                    
        else:
            success = False
            
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
        
        success = True
        next_state = None
        self._debug_print(event)
        
        if event == 'EVENT_ENTER':
            
            # Announce the state change to agent.            
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_CONNECTING'}
            self.send(self.proc_supid,'driver_event_occurred',content)
            
            self.getConnected()
            
        elif event == 'EVENT_EXIT':
            pass
                    
        elif event == 'EVENT_CONNECTION_COMPLETE':
            next_state = 'STATE_UPDATE_PARAMS'
                    
        elif event == 'EVENT_CONNECTION_FAILED':
            # Error message to agent here.
            next_state = 'STATE_DISCONNECTED'

        else:
            success = False

        return (success,next_state)


    def state_handler_disconnecting(self,event,params):
        """
        Event handler for STATE_DISCONNECTING.
        Events handled:
        EVENT_ENTER: Attempt to close connection to instrument.
        EVENT_EXIT: Pass.        
        EVENT_DISCONNECT_COMPLETE: Switch to STATE_DISCONNECTED.
        """
        
        success = True
        next_state = None
        self._debug_print(event)
            
        if event == 'EVENT_ENTER':
            
            # Announce the state change to agent.            
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_DISCONNECTED'}
            self.send(self.proc_supid,'driver_event_occurred',content)            
            
            self.getDisconnected()
            
        elif event == 'EVENT_EXIT':
            pass
                    
        elif event == 'EVENT_DISCONNECT_COMPLETE':
            next_state = 'STATE_DISCONNECTED'

        else:
            success = False

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
        
        success = True
        next_state = None
        self._debug_print(event)

        if event == 'EVENT_ENTER':
            
            # Announce the state change to agent.            
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_CONNECTED'}
            self.send(self.proc_supid,'driver_event_occurred',content)            
            
            # If we enter connected with the connection complete deferred
            # defined we are establishing the initial connection in response
            # to a connect command. Send the reply to indicate successful
            # connection.
            if self._connection_complete_deferred:
                d,self._connection_complete_deferred = self._connection_complete_deferred,None
                reply = {'success':['OK'],'result':None}
                d.callback(reply)
            
        elif event == 'EVENT_EXIT':
            pass
            
        elif event == 'EVENT_DISCONNECT':
            next_state = 'STATE_DISCONNECTING'
                    
        elif event == 'EVENT_SET':
            next_state = 'STATE_SET'
            
        elif event == 'EVENT_ACQUIRE_SAMPLE':
            next_state = 'STATE_ACQUIRE_SAMPLE'
            
        elif event == 'EVENT_START_AUTOSAMPLE':
            next_state = 'STATE_AUTOSAMPLE'
            
        elif event == 'EVENT_TEST':
            next_state = 'STATE_TEST'
            
        elif event == 'EVENT_CALIBRATE':
            next_state = 'STATE_CALIBRATE'
            
        elif event == 'EVENT_RESET':
            next_state = 'STATE_RESET'
            
        elif event == 'EVENT_DATA_RECEIVED':
            pass
                
        else:
            success = False

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
        
        success = True
        next_state = None
        self._debug_print(event)

        if event == 'EVENT_ENTER':
                        
            # Announce the state change to agent.            
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_ACQUIRE_SAMPLE'}
            self.send(self.proc_supid,'driver_event_occurred',content)                                    
                        
            # Initialize data buffer and copy device command buffer over
            # from the acquire command spec.
            command_spec = self._driver_command_buffer[0]
            self._data_lines = []
            self._sample_buffer = []
            self._device_command_buffer = command_spec.device_command_buffer
                        
            # Start the looping wakeup.
            self._start_wakeup(2.0)
            
        elif event == 'EVENT_EXIT':

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
                
            command_spec.set_success(['OK'],errors['ACQUIRE_SAMPLE_ERR'])
            command_spec.do_reply()
            self._sample_buffer = []        
                    
                    
        elif event == 'EVENT_PROMPTED':
                        
                    
            # Cancel looping wakeup if active.
            self._stop_wakeup()
                
            # Pop the device command.
            try:
                
                cmd = self._device_command_buffer.pop(0)
            
            # If no further commands, parse output, set result and switch
            # state.
            except IndexError:
                    
                next_state = 'STATE_CONNECTED'

            # Write the device command.
            else:
                
                self._write_command(cmd+SBE37_NEWLINE)
                                
        elif event == 'EVENT_DATA_RECEIVED':

            #content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
            #           'value':'STATE_UNCONFIGURED'}
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
                content = {'type':'DATA_RECEIVED',
                           'transducer':'CHAN_INSTRUMENT','value':samples}
                self.send(self.proc_supid,'driver_event_occurred',content)                                                
            
            
            
        else:
            success = False
        
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
        
        success = True
        next_state = None
        self._debug_print(event)

        if event == 'EVENT_ENTER':

            # Announce the state change to agent.            
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_AUTOSAMPLE'}
            self.send(self.proc_supid,'driver_event_occurred',content)                                    

            # Clear the data buffer and copy the device command buffer
            # from the autosample command spec.
            command_spec = self._driver_command_buffer[0]
            self._data_lines = []
            self._sample_buffer = []
            self._device_command_buffer = command_spec.device_command_buffer
                        
            # Start the looping wakeup.
            self._start_wakeup(2.0)

            
        elif event == 'EVENT_EXIT':

            # Pop the command spec.                
            command_spec = self._driver_command_buffer.pop(0)

            # If this is a stop command with getdata arg, populate the reply.
            drv_cmd = command_spec.command
            if list(drv_cmd) == ['DRIVER_CMD_STOP_AUTO_SAMPLING','GETDATA']:
                if len(self._sample_buffer)==0:
                    command_spec.errors = True
                elif len(self._sample_buffer)==1:
                    command_spec.reply['result'] = self._sample_buffer[0]
                else:
                    command_spec.reply['result'] = self._sample_buffer

            # Clear the data buffer, set success and fire reply deferred.
            self._data_lines = []
            self._sample_buffer = []
            command_spec.set_success(['OK'],errors['ACQUIRE_SAMPLE_ERR'])
            command_spec.do_reply()
                                    
        elif event == 'EVENT_PROMPTED':
            
            # Cancel looping wakeup if active.
            self._stop_wakeup()

            # Get the driver command.                
            command_spec = self._driver_command_buffer[0]
            drv_cmd = command_spec.command

            if drv_cmd[0] == 'DRIVER_CMD_START_AUTO_SAMPLING':

                # Pop the pending autosample device command.
                try:
                    cmd = self._device_command_buffer.pop(0)

                except IndexError:
                    pass

                # Write autosample device command, remove command spec
                # from buffer, set result and fire the reply deferred.
                else:
                    self._write_command(cmd+SBE37_NEWLINE)
                    self._driver_command_buffer.pop(0)
                    command_spec.set_success(['OK'],errors['ACQUIRE_SAMPLE_ERR'])
                    command_spec.do_reply()
                    
                                   
            elif drv_cmd[0] == 'DRIVER_CMD_STOP_AUTO_SAMPLING':

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
                        next_state = 'STATE_CONNECTED'

                # Write the autosample stop device command.                
                else:
                    self._write_command(cmd+SBE37_NEWLINE)
                    
                                        
        elif event == 'EVENT_STOP_AUTOSAMPLE':

            # Do not clear data buffer, copy over the stop autosample
            # device command buffer.
            command_spec = self._driver_command_buffer[0]
            self._device_command_buffer = command_spec.device_command_buffer

            # Start the looping wakeup.
            self._start_wakeup(2.0)
            
                        
        elif event == 'EVENT_DATA_RECEIVED':
            
            # Parse the data buffer for sample output. Publish these as
            # appropriate, and package them in the reply if the option
            # has been set.
            samples = self._parse_sample_output()
            if len(samples)>0:
                self._sample_buffer += samples
                if len(samples)==1:
                    samples = samples[0]
                self._debug_print('received samples',samples)
                content = {'type':'DATA_RECEIVED',
                           'transducer':'CHAN_INSTRUMENT','value':samples}
                self.send(self.proc_supid,'driver_event_occurred',content)                                                
            
        else:
            
            success = False
        
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
        
        success = True
        next_state = None
        self._debug_print(event)

        if event == 'EVENT_ENTER':
            
            # Announce the state change to agent.            
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_UPDATE_PARAMS'}
            self.send(self.proc_supid,'driver_event_occurred',content)                                    
              
            # Clear data buffer and populate device command buffer directly.
            self._data_lines = []
            self._device_command_buffer = ['DS','DC']
           
            # Start looping wakeup.
            self._start_wakeup(2.0)

            
        elif event == 'EVENT_EXIT':
            
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
            paramdict = self.get_parameters()
            content = {'type':'CONFIG_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':paramdict}
            self.send(self.proc_supid,'driver_event_occurred',content)                                                
                                
                    
        elif event == 'EVENT_PROMPTED':
            
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
                    next_state = 'STATE_CONNECTED'
    
                # Write command to device.
                else:
                    self._write_command(cmd+SBE37_NEWLINE)
                

        elif event == 'EVENT_DATA_RECEIVED':
            pass
                
        else:
            success = False

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
        
        success = True
        next_state = None
        self._debug_print(event)

        if event == 'EVENT_ENTER':
            
            # Announce the state change to agent.            
            content = {'type':'STATE_CHANGE','transducer':'CHAN_INSTRUMENT',
                       'value':'STATE_SET'}
            self.send(self.proc_supid,'driver_event_occurred',content)                                                
            
            # Clear the data buffer and copy the device command buffer over
            # from the command spec.
            command_spec = self._driver_command_buffer[0]
            self._data_lines = []
            self._device_command_buffer = command_spec.device_command_buffer
                            
            # Start the looping wakeup.
            self._start_wakeup(2.0)

            
        elif event == 'EVENT_EXIT':
            
            # Clear the data buffer.
            self._data_lines = []
                    
        elif event == 'EVENT_PROMPTED':
            
            #print 'line buf: '+self._line_buffer+'xx'
            # Cancel the looping wakeup if active.
            self._stop_wakeup()
            
            # Validate previous response.
            command_spec = self._driver_command_buffer[0]
            if self._line_buffer == SBE37_PROMPT:
                prev_result = ['OK']
            else:
                prev_result = errors['BAD_DRIVER_COMMAND']
            command_spec.set_previous_result(prev_result)
            

            # Pop the pending device set command.
            try:
                
                (set_key,set_val,set_cmd) = self._device_command_buffer.pop(0)

            # If no remainind device set commands, set success and switch
            # state.
            except IndexError:
                
                command_spec.set_success(['OK'],errors['SET_DEVICE_ERR'])
                next_state = 'STATE_UPDATE_PARAMS'

            # Write the device set command.                
            else:
                
                command_spec.previous_key = set_key
                #print 'writing command '+set_cmd
                self._write_command(set_cmd+SBE37_NEWLINE)

        elif event == 'EVENT_DATA_RECEIVED':
            pass                
                        
        else:
            success = False
            
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
        self.fsm.start('STATE_UNCONFIGURED')
                

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
            self.fsm.on_event('EVENT_CONNECTION_COMPLETE')
        else:
            self.fsm.on_event('EVENT_CONNECTION_FAILED')


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
        self.fsm.on_event('EVENT_DISCONNECT_COMPLETE')


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
        if SBE37_NEWLINE in self._line_buffer:
            lines = self._line_buffer.split(SBE37_NEWLINE)
            self._line_buffer = lines[-1]
            self._data_lines += lines[0:-1]
            new_lines = True

        # If the linebuffer ends with a prompt, extract and append the
        # prefix data to the data buffer. Keep the prompt in the line buffer.
        if self._line_buffer.endswith(SBE37_PROMPT):
            self._data_lines.append(self._line_buffer.replace(SBE37_PROMPT,''))
            self._line_buffer = SBE37_PROMPT
            new_lines = True
        
        # If the line buffer ends with a bad command prompt, extract and
        # append the prefix data to the data buffer. Keep the prompt in the
        # line buffer.
        elif self._line_buffer.endswith(SBE37_BAD_COMMAND):
            self._data_lines.append(self._line_buffer.
                                    replace(SBE37_BAD_COMMAND,''))
            self._line_buffer = SBE37_BAD_COMMAND
            new_lines = True
        
        # If new complete lines are detected, send an EVENT_DATA_RECEIVED.
        if new_lines:
            self.fsm.on_event('EVENT_DATA_RECEIVED')
        
        # If a normal or bad command prompt is detected, send an
        # EVENT_PROMPTED
        if (self._line_buffer == SBE37_PROMPT or
            self._line_buffer == SBE37_BAD_COMMAND):
            self.fsm.on_event('EVENT_PROMPTED')
            
        # If a stop autosample type prompt is detected, send an
        # EVENT_PROMPTED.
        elif (self._line_buffer == '' and
              (len(self._data_lines)>0 and self._data_lines[-1] == SBE37_PROMPT)):
            self.fsm.on_event('EVENT_PROMPTED')


    ###########################################################################
    # Agent interface methods.
    ###########################################################################


    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute a driver command. Commands may be
        common or specific to the device, with specific commands known through
        knowledge of the device or a previous get_capabilities query.
        @param content A dict
            {'channels':[chan_arg,...,chan_arg],
            'command':[command,arg,...,argN]),}
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
            reply['success'] = errors['REQUIRED_PARAMETER']
            yield self.reply_ok(msg,reply)
            return
        if not channels:
            reply['success'] = errors['REQUIRED_PARAMETER']
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
        if command[0] not in sbe37_command_list:
            reply['success'] = errors['UNKNOWN_COMMAND']
            yield self.reply_ok(msg,reply)
            return
        
        for chan in channels:
            if chan not in sbe37_channel_list:
                reply['success'] = errors['UNKNOWN_CHANNEL']
                yield self.reply_ok(msg,reply)
                return

        drv_cmd = command[0]
        
        # Process acquire sample command.
        if drv_cmd == 'DRIVER_CMD_ACQUIRE_SAMPLE':

            # Create command spec and set event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = 'EVENT_ACQUIRE_SAMPLE'
            
            # The acquire command only applies to the instrument as a whole.
            # Fail if the channel is not set properly.
            if len(channels)>1 or channels[0] != 'CHAN_INSTRUMENT':
                reply['success'] = errors['INVALID_CHANNEL']
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
                    reply['success'] = errors['INVALID_COMMAND']
                    yield self.reply_ok(msg,reply)
                    return
                else:
                    command_spec.device_command_buffer = command[1]
            
            # If the command has a length greater than 2, fail.      
            else:
                reply['success'] = errors['INVALID_COMMAND']
                yield self.reply_ok(msg,reply)
                return

            # Set up the reply deferred and fire the command event.  
            reply = yield self._process_command(command_spec,event,timeout)


        # Process start autosampling command.
        elif drv_cmd == 'DRIVER_CMD_START_AUTO_SAMPLING':

            # Create command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = 'EVENT_START_AUTOSAMPLE'

            # The acquire command only applies to the instrument as a whole.
            # Fail if channel not set properly.
            if len(channels)>1 or channels[0] != 'CHAN_INSTRUMENT':
                reply['success'] = errors['INVALID_CHANNEL']
                yield self.reply_ok(msg,reply)
                return

            # Append the sbe37 start command to the device command buffer.                                
            command_spec.device_command_buffer = ['STARTNOW']                

            # Set up the reply deferred and fire the command event.  
            reply = yield self._process_command(command_spec,event,timeout)


        # Process stop autosampling command.
        elif drv_cmd == 'DRIVER_CMD_STOP_AUTO_SAMPLING':
            
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = 'EVENT_STOP_AUTOSAMPLE'

            # The acquire command only applies to the instrument as a whole.
            # Fail if channel not set properly.
            if len(channels)>1 or channels[0] != 'CHAN_INSTRUMENT':
                reply['success'] = errors['INVALID_CHANNEL']
                yield self.reply_ok(msg,reply)
                return
            
            # The command has one optional argument,
            # that retrieves the autosampled data in the stop reply.
            if len(command)>2:
                reply['success'] = errors['INVALID_COMMAND']
                yield self.reply_ok(msg,reply)
                return

            if len(command)==2 and command[1]!='GETDATA':
                reply['success'] = errors['INVALID_COMMAND']
                yield self.reply_ok(msg,reply)
                return

            # Append the sbe37 command to the device command buffer.                                
            command_spec.device_command_buffer = ['STOP']                

            # Set up the reply deferred and fire the command event.  
            reply = yield self._process_command(command_spec,event,timeout)


        # Process test command.
        elif drv_cmd == 'DRIVER_CMD_TEST':
            
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = 'EVENT_RESET'
            
            # Return not implemented reply.
            reply['success'] = errors['NOT_IMPLEMENTED']
            yield self.reply_ok(msg,reply)
            return

        elif drv_cmd == 'DRIVER_CMD_CALIBRATE':
            
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = 'EVENT_RESET'
            
            # Return not implemented reply.
            reply['success'] = errors['NOT_IMPLEMENTED']
            yield self.reply_ok(msg,reply)
            return

        elif drv_cmd == 'DRIVER_CMD_RESET':
                       
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            event = 'EVENT_RESET'

            # Return not implemented reply.
            reply['success'] = errors['NOT_IMPLEMENTED']
            yield self.reply_ok(msg,reply)
            return

        elif drv_cmd == 'DRIVER_CMD_TEST_ERRORS':
                       
            # Create a command spec and set the event to fire.
            command_spec = DeviceCommandSpecification(command)
            
            reply = {'success':['OK'],'result':{}}
            
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
            reply['success'] = errors['INVALID_COMMAND']
            yield self.reply_ok(msg,reply)
            return
        
        yield self.reply_ok(msg,reply)        


    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        Get configuration parameters from the device. 
        @param content A list [(chan_arg,param_arg),...,(chan_arg,param_arg)].
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
        @param content A dict {(chan_arg,param_arg):val,...,
            (chan_arg,param_arg):val}.
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
        command_spec = DeviceCommandSpecification(['DRIVER_CMD_SET'])        
        event = 'EVENT_SET'
        
        # Populate the device command buffer with the equivalent
        # sbe37 set commands.
        device_command_buffer = []
        for (chan,param) in params.keys():
            val = params[(chan,param)]
            if self.parameters.get((chan,param),None):
                str_val = self.parameters[(chan,param)]['parser'].tostring(val)
                if str_val:
                    device_command_buffer.append(((chan,param),val,
                        param+'='+str_val))
                else:
                    # Parser error creating value string.
                    pass
            else:
                command_spec.reply['result'][(chan,param)] = errors['INVALID_PARAMETER']
                command_spec.errors = True

        # If no valid commands, reply error.
        if len(device_command_buffer)==0:
            reply = command_spec.reply
            reply['success'] = errors['SET_DEVICE_ERR']
            
        # Otherwise, set device command buffer and process command.
        else:
            command_spec.device_command_buffer = device_command_buffer
            reply = yield self._process_command(command_spec,event)

        yield self.reply_ok(msg,reply)


    @defer.inlineCallbacks
    def op_get_metadata(self, content, headers, msg):
        """
        Retrieve metadata for the device, its transducers and parameters.
        @param content A list:[(chan_arg,param_arg,meta_arg),...,
            (chan_arg,param_arg,meta_arg)] specifying the metadata to retrieve.
        @retval Reply message with a dict {'success':success,'result':
                {(chan_arg,param_arg,meta_arg):(success,val),...,
                chan_arg,param_arg,meta_arg):(success,val)}}.        
        """
        
        assert(isinstance(content,dict)), 'Expected dict content.'
        params = content.get('params',None)
        assert(isinstance(params,list)), 'Expected list params.'
        assert(all(map(lambda x:isinstance(tuple),params))), 'Expected tuple arguments'
        
        # Timeout not implemented for this op.
        timeout = content.get('timeout',None)
        if timeout != None:
            assert(isinstance(timeout,int)), 'Expected integer timeout'
            assert(timeout>0), 'Expected positive timeout'
            pass
        
        
        # The method is not implemented.
        reply = {'success':errors['NOT_IMPLEMENTED'],'result':None}
        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_get_status(self, content, headers, msg):
        """
        Obtain the status of the device. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param content A list [(chan_arg,status_arg),...,
            (chan_arg,status_arg)] specifying the status arguments to query.
        @retval A reply message with a dict
            {'success':success,'result':{(chan_arg,status_arg):(success,val),
                ...,chan_arg,status_arg):(success,val)}}.
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

        
        # The method is not implemented.
        reply = {'success':errors['NOT_IMPLEMENTED'],'result':None}
        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_initialize(self, content, headers, msg):
        """
        Restore driver to a default, unconfigured state.
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
        success = self.fsm.on_event('EVENT_INITIALIZE')
        
        # Set success and send reply. Unsuccessful initialize means the
        # event is not handled in the current state.
        if not success:
            reply['success'] = errors['INCORRECT_STATE']
        else:
            reply['success'] = ['OK']
 
        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_configure(self, content, headers, msg):
        """
        Configure the driver to establish communication with the device.
        @param content a dict containing required and optional
            configuration parameters.
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
        if reply['success'][0] != 'OK':
            yield self.reply_ok(msg,reply)
            return
        
        # Fire EVENT_CONFIGURE with the validated configuration parameters.
        # Set the error message if the event is not handled in the current
        # state.
        success = self.fsm.on_event('EVENT_CONFIGURE',params)
        if not success:
            reply['success'] = errors['INCORRECT_STATE']
            
        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_connect(self, content, headers, msg):
        """
        Establish connection to the device.
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
        @retval The current instrument state, from sbe37_state_list
        (see ion.agents.instrumentagents.instrument_agent_constants.
            device_state_list for the common states.)
        """
        
        # Get current state from the state machine and reply.
        cur_state = self.fsm.current_state
        yield self.reply_ok(msg, cur_state)

    @defer.inlineCallbacks
    def op_test_stub():
        pass


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
        if success[0] != 'OK':
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
            return errors['REQUIRED_PARAMETER']

        # Validate port number.
        if not isinstance(_ipport,int) or _ipport <0 or _ipport > 65535:
            return errors['INVALID_PARAM_VALUE']
        
        # Validate ip address.
        # TODO: Add logic to veirfy string format.
        if not isinstance(_ipaddr,str): 
            return errors['INVALID_PARAM_VALUE']

        return ['OK']        

        
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
        success = self.fsm.on_event('EVENT_CONNECT')
        if not success:
            reply = {'success':errors['INCORRECT_STATE'],'result':None}
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
        success = self.fsm.on_event('EVENT_DISCONNECT')
        if not success:
            reply = {'success':errors['INCORRECT_STATE'],'result':None}
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
        if self.fsm.on_event(event):
            return d
        else:
            reply = {'success':errors['INCORRECT_STATE'],'result':None}
            del self._driver_command_buffer[-1]
            return reply            
        
            
    @defer.inlineCallbacks
    def publish(self, topic, transducer, data):
        """
        """
        yield

    ###########################################################################
    # Other.
    ###########################################################################

    def get_parameters(self):
        """
        """
        paramdict = dict(map(lambda x: (x[0],x[1]['value']),self.parameters.items()))
        return paramdict

    def _wakeup(self,wakeup_string=SBE37_NEWLINE,reps=1):
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
            if chan == 'all' and param == 'all':
                for (key,val) in self.parameters.iteritems():
                    result[key] = (['OK'],val['value'])

            elif chan == 'all':

                # Invalid parameter name.
                if param not in params_list:
                    result[(chan,param)] = (errors['INVALID_PARAMETER'],None)

                # Retrieve a valid named parameter for all channels.
                else:
                    for (key,val) in self.parameters.iteritems():
                        if key[1] == param:
                            result[key] = (['OK'],val['value'])

            elif param == 'all':

                # Invalid channel name.
                if chan not in channels_list:
                    result[(chan,param)] = (errors['INVALID_PARAMETER'],None)
                    
                # Retrieve all parameters for a valid named channel.
                else:
                    for (key,val) in self.parameters.iteritems():
                        if key[0] == chan:
                            result[key] = (['OK'],val['value'])
                            
            # Retrieve named channel-parameters
            else:

                val = self.parameters.get((chan,param),None)

                # Invalid channel or parameter name.
                if val == None:
                    result[(chan,param)] = (errors['INVALID_PARAMETER'],None)
                    get_errors = True

                # Valid channel parameter names.
                else:
                    result[(chan,param)] = (['OK'],val['value'])
        
        # Set up reply success and return.
        if get_errors:
            reply['success'] = errors['GET_DEVICE_ERR']
        else:
            reply['success'] = ['OK']

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
            sample_data['sound velocity'] = float(match.group(7))
        elif match.group(5):
            if self.parameters.get('OUTPUTSAL')['value']:
                sample_data['salinity'] = float(match.group(5))
            elif self.parameters.get('OUTPUTSV')['value']:
                sample_data['sound velocity'] = match.group(5)
        
        # Extract date and time if present.
        sample_time = None
        if  match.group(8):
            sample_time = time.strptime(match.group(8),', %d %b %Y, %H:%M:%S')
            
        elif match.group(15):
            sample_time = time.strptime(match.group(15),', %m-%d-%Y, %H:%M:%S')
        
        if sample_time:
            sample_data['date'] = (sample_time[2],sample_time[1],sample_time[0])
            sample_data['time'] = (sample_time[3],sample_time[4],sample_time[5])            
        
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
            print self.fsm.current_state + '  ' + event
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

