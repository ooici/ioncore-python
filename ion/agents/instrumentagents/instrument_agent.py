#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/instrument_agent.py
@author Steve Foley
@brief Instrument Agent, Driver, and Client class definitions
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.agents.resource_agent import ResourceAgent
from ion.agents.resource_agent import ResourceAgentClient
from ion.core.exception import ReceivedError
from ion.services.dm.distribution.pubsub_service import PubSubClient
from ion.data.dataobject import ResourceReference, DataObject
from ion.core.process.process import Process, ProcessClient, ProcessFactory, ProcessDesc
from ion.resources.ipaa_resource_descriptions import InstrumentAgentResourceInstance
from ion.resources.dm_resource_descriptions import PublisherResource
from uuid import uuid4
import ion.util.procutils as pu


"""
Constants/Enumerations for tags in capabilities dict structures.
"""
ci_commands             = 'ci_commands'
ci_parameters           = 'ci_parameters'
instrument_commands     = 'instrument_commands'
instrument_parameters   = 'instrument_parameters'


"""
Observatory state names.
"""
ci_state_list = [
    'CI_STATE_UNKNOWN',
    'CI_STATE_POWERED_DOWN',
    'CI_STATE_UNINITIALIZED',
    'CI_STATE_INACTIVE',
    'CI_STATE_STOPPED',
    'CI_STATE_IDLE',
    'CI_STATE_OBSERVATORY_MODE',
    'CI_STATE_DIRECT_ACCESS_MODE'
]

"""
Observatory transition names.
"""
ci_transition_list = [
    'CI_TRANS_INITIALIZE',
    'CI_TRANS_RESET',
    'CI_TRANS_GO_ACTIVE',
    'CI_TRANS_GO_INACTIVE',
    'CI_TRANS_CLEAR',
    'CI_TRANS_RESUME',
    'CI_TRANS_RUN',
    'CI_TRANS_PAUSE',
    'CI_TRANS_OBSERVATORY_MODE',
    'CI_TRANS_DIRECT_ACCESS_MODE'
    
]


"""
Observatory commands names.
"""
ci_command_list = [
    'CI_CMD_STATE_TRANSITION',
    'CI_CMD_TRANSMIT_DATA'
]

"""
Parameter names for instrument agents.
"""
ci_param_list = [
    'CI_PARAM_DATA_TOPICS',
    'CI_PARAM_EVENT_TOPICS',
    'CI_PARAM_STATE_TOPICS',
    'CI_PARAM_DRIVER_ADDRESS',
    'CI_PARAM_RESOURCE_ID',
    'CI_PARAM_TIME_SOURCE',
    'CI_PARAM_CONNECTION_METHOD',
    'CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT',
    'CI_PARAM_MAX_TRANSACTION_TIMEOUT',
    'CI_PARAM_TRANSACTION_EXPIRE_TIMEOUT'
]

"""
List of observatory status names.
"""
ci_status_list = [
    'CI_STATUS_AGENT_STATE',
    'CI_STATUS_CHANNEL_NAMES',
    'CI_STATUS_INSTRUMENT_CONNECTION_STATE',
    'CI_STATUS_ALARMS',
    'CI_STATUS_TIME_STATUS',
    'CI_STATUS_BUFFER_SIZE',
    'CI_STATUS_AGENT_VERSION',
    'CI_STATUS_DRIVER_VERSION'    
]

"""
Agent parameter and metadata types.
"""
ci_param_types = [
    'CI_TYPE_DATATYPE',             # This type.
    'CI_TYPE_INT',                  # int.
    'CI_TYPE_FLOAT',                # float.
    'CI_TYPE_BOOL',                 # bool.
    'CI_TYPE_STRING',               # str.
    'CI_TYPE_INT_RANGE',            # (int,int).
    'CI_TYPE_FLOAT_RANGE',          # (float,float).
    'CI_TYPE_TIMESTAMP',            # (int seconds,int nanoseconds).
    'CI_TYPE_TIME_DURATION',        # TBD.
    'CI_TYPE_PUBSUB_TOPIC_DICT',    # dict of topic strings.
    'CI_TYPE_RESOURCE_ID',          # str (possible validation).
    'CI_TYPE_ADDRESS',              # str (possible validation).
    'CI_TYPE_ENUM'                  # str with valid values.
]


"""
Used by the existing drivers...need to fix.
"""
publish_msg_type = {
    'Error':'Error',
    'StateChange':'StateChange',
    'ConfigChange':'ConfigChange',
    'Data':'Data',
    'Event':'Event'
}

"""
Publish message types.
"""
publish_msg_types = [
    'PUBLISH_MSG_ERROR',                        
    'PUBLISH_MSG_STATE_CHANGE',
    'PUBLISH_MSG_CONIFG_CHANGE',
    'PUBLISH_MSG_DATA',
    'PUBLISH_MSG_EVENT'    
]

"""
Time source of device fronted by agent.
"""
time_sources = [
    'TIME_NOT_SPECIFIED',                       #
    'TIME_PTP_DIRECT',                          # IEEE 1588 PTP connection directly supported.
    'TIME_NTP_UNICAST',                         # NTP unicast to the instrument.
    'TIME_NTP_BROADCAST',                       # NTP broadcast to the instrument.
    'TIME_LOCAL_OSCILLATOR',                    # Device has own clock.
    'TIME_DRIVER_SET_INTERVAL'                  # Driver sets clock at interval.
]

"""
Connection method to agent and device.
"""
connection_methods = [
    'CONNECTION_NOT_SPECIFIED',                #
    'CONNECTION_OFFLINE',                      # Device offline.
    'CONNECTION_CABLED_OBSERVATORY',           # Accessible through cabled observatory, available full time.
    'CONNECTION_SHORE_NETWORK',                # Connected through full time shore connection.
    'CONNECTION_PART_TIME_SCHEDULED',          # Comes online on scheduled basis. Outages normal.
    'CONNECTION_PART_TIME_RANDOM'              # Comes online as needed. Outages normal.
]

"""
Observatory alarm conditions.
"""
status_alarms = {
    'ALARM_CANNOT_PUBLISH'              : ('ALARM_CANNOT_PUBLISH','Attempted to publish but cannot.'),
    'ALARM_INSTRUMENT_UNREACHABLE'      : ('ALARM_INSTRUMENT_UNREACHABLE','Instrument cannot be contacted when it should be.'),
    'ALARM_MESSAGING_ERROR'             : ('ALARM_MESSAGING_ERROR','Error when sending messages.'),
    'ALARM_HARDWARE_ERROR'              : ('ALARM_HARDWARE_ERROR','Hardware problem detected.'),
    'ALARM_UNKNOWN_ERROR'               : ('ALARM_UNKNOWN_ERROR','An unknown error has occurred.')   
}
    
"""
Names of observatory and device capability lists.
"""
capabilities_list = [
    'CAP_OBSERVATORY_COMMANDS',         # Common and specific observatory command names.
    'CAP_OBSERVATORY_PARAMETERS',       # Common and specific observatory parameter names.
    'CAP_OBSERVATORY_STATUSES',         # Common and specific observatory status names.
    'CAP_METADATA',                     # Common and specific metadata names.
    'CAP_DEVICE_COMMANDS',              # Common and specific device command names.
    'CAP_DEVICE_PARAMETERS',            # Common and specific device parameter names.
    'CAP_DEVICE_STATUSES'               # Common and specific device status names.
]

"""
Parameter names for agent and device metadata.
"""
metadata_list = [
    'META_DATATYPE',
    'META_PHYSICAL_PARAMETER_TYPE',
    'META_MINIMUM_VALUE',
    'META_MAXIMUM_VALUE',    
    'META_UNITS',
    'META_UNCERTAINTY',
    'META_LAST_CHANGE_TIMESTAMP',
    'META_WRITABLE',
    'META_VALID_VALUES',    
    'META_FRIENDLY_NAME',
    'META_DESCRIPTION'
]

"""
Instrument agent observatory metadata.
"""
ci_param_metadata = {
    
    'CI_PARAM_DATA_TOPICS' :
        {'META_DATATYPE':'CI_PUBSUB_TOPIC_DICT',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_FRIENDLY_NAME':'Data Topics'},
    'CI_PARAM_EVENT_TOPICS' :
        {'META_DATATYPE':'CI_PUBSUB_TOPIC_DICT',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_FRIENDLY_NAME':'Event Topics'},
    'CI_PARAM_STATE_TOPICS' :
        {'META_DATATYPE':'CI_PUBSUB_TOPIC_DICT',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_FRIENDLY_NAME':'State Topics'},
    'CI_PARAM_DRIVER_ADDRESS' :
        {'META_DATATYPE':'CI_TYPE_ADDRESS',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_FRIENDLY_NAME':'Driver Address'},
    'CI_PARAM_RESOURCE_ID' :
        {'META_DATATYPE':'CI_TYPE_RESOURCE_ID',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_FRIENDLY_NAME':'Resource ID'},
    'CI_PARAM_TIME_SOURCE' :
        {'META_DATATYPE':'CI_TYPE_ENUM',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_VALID_VALUES':time_sources,
         'META_FRIENDLY_NAME':'Time Source'},
    'CI_PARAM_CONNECTION_METHOD' :
        {'META_DATATYPE':'CI_TYPE_ENUM',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_VALID_VALUES':connection_methods,
         'META_FRIENDLY_NAME':'Connection Method'},
    'CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT' :
        {'META_DATATYPE':'CI_TYPE_INT',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_MINIMUM_VALUE':0,
         'META_UNITS':'Seconds',
         'META_FRIENDLY_NAME':'Default Transaction Timeout'},
    'CI_PARAM_MAX_TRANSACTION_TIMEOUT' :
        {'META_DATATYPE':'CI_TYPE_INT',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_MINIMUM_VALUE':0,
         'META_UNITS':'Seconds',
         'META_FRIENDLY_NAME':'Max Transaction Timeout'},
    'CI_PARAM_TRANSACTION_EXPIRE_TIMEOUT' :
        {'META_DATATYPE':'CI_TYPE_INT',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_MINIMUM_VALUE':0,
         'META_UNITS':'Seconds',
         'META_FRIENDLY_NAME':'Transaction Expire Timeout'}    
}



"""
Agent errors.
"""
errors = {
    'INVALID_DESTINATION'       : ['ERROR','INVALID_DESTINATION','Intended destination for a message or operation is not valid.'],
    'TIMEOUT'                   : ['ERROR','TIMEOUT','The message or operation timed out.'],
    'NETWORK_FAILURE'           : ['ERROR','NETWORK_FAILURE','A network failure has been detected.'],
    'NETWORK_CORRUPTION'        : ['ERROR','NETWORK_CORRUPTION','A message passing through the network has been determined to be corrupt.'],
    'OUT_OF_MEMORY'	        : ['ERROR','OUT_OF_MEMORY','There is no more free memory to complete the operation.'],
    'LOCKED_RESOURCE'	        : ['ERROR','LOCKED_RESOURCE','The resource being accessed is in use by another exclusive operation.'],
    'RESOURCE_NOT_LOCKED'       : ['ERROR','RESOURCE_NOT_LOCKED','Attempted to unlock a free resource.'],
    'RESOURCE_UNAVAILABLE'      : ['ERROR','RESOURCE_UNAVAILABLE','The resource being accessed is unavailable.'],
    'TRANSACTION_REQUIRED'      : ['ERROR','TRANSACTION_REQUIRED','The operation requires a transaction with the agent.'],
    'UNKNOWN_ERROR'             : ['ERROR','UNKNOWN_ERROR','An unknown error has been encountered.'],
    'PERMISSION_ERROR'          : ['ERROR','PERMISSION_ERROR','The user does not have the correct permission to access the resource in the desired way.'],
    'INVALID_TRANSITION'        : ['ERROR','INVALID_TRANSITION','The transition being requested does not apply for the current state.'],
    'INCORRECT_STATE'           : ['ERROR','INCORRECT_STATE','The operation being requested does not apply to the current state.'],
    'UNKNOWN_TRANSITION'        : ['ERROR','UNKNOWN_TRANSITION','The specified state transition does not exist.'],
    'CANNOT_PUBLISH'	        : ['ERROR','CANNOT_PUBLISH','An attempt to publish has failed.'],
    'INSTRUMENT_UNREACHABLE'    : ['ERROR','INSTRUMENT_UNREACHABLE','The agent cannot communicate with the device.'],
    'MESSAGING_ERROR'           : ['ERROR','MESSAGING_ERROR','An error has been encountered during a messaging operation.'],
    'HARDWARE_ERROR'            : ['ERROR','HARDWARE_ERROR','An error has been encountered with a hardware element.'],
    'WRONG_TYPE'                : ['ERROR','WRONG_TYPE','The type of operation is not valid in the current state.'],
    'INVALID_COMMAND'           : ['ERROR','INVALID_COMMAND','The command is not valid in the given context.'],    
    'UNKNOWN_COMMAND'           : ['ERROR','UNKNOWN_COMMAND','The command is not recognized.'],
    'NOT_IMPLEMENTED'           : ['ERROR','NOT_IMPLEMENTED','The command is not implemented.'],
    'INVALID_TRANSACTION_ID'    : ['ERROR','INVALID_TRANSACTION_ID','The transaction ID is not a valid value.'],
    'INVALID_DRIVER'            : ['ERROR','INVALID_DRIVER','Driver or driver client invalid.'],
    'GET_OBSERVATORY_ERR'       : ['ERROR','GET_OBSERVATORY_ERR','Could not retrieve all parameters.'],
    'EXE_OBSERVATORY_ERR'       : ['ERROR','EXE_OBSERVATORY_ERR','Could not execute observatory command.'],
    'SET_OBSERVATORY_ERR'       : ['ERROR','SET_OBSERVATORY_ERR','Could not set all parameters.'],
    'PARAMETER_READ_ONLY'       : ['ERROR','PARAMETER_READ_ONLY','Parameter is read only.'],
    'INVALID_PARAMETER'         : ['ERROR','INVALID_PARAMETER','The parameter is not available.'],
    'INVALID_PARAM_VALUE'       : ['ERROR','INVALID_PARAM_VALUE','The parameter value is out of range.'],
    'INVALID_METADATA'          : ['ERROR','INVALID_METADATA','The metadata parameter is not available.'],
    'NO_PARAM_METADATA'         : ['ERROR','NO_PARAM_METADATA','The parameter has no associated metadata.'],
    'INVALID_STATUS'            : ['ERROR','INVALID_STATUS','The status parameter is not available.'],
    'INVALID_CAPABILITY'        : ['ERROR','INVALID_CAPABILITY','The capability parameter is not available.']
}


class InstrumentDriver(Process):
    """
    A base driver class. This is intended to provide the common parts of
    the interface that instrument drivers should follow in order to use
    common InstrumentAgent methods. This should never really be instantiated.
    """
    def op_fetch_params(self, content, headers, msg):
        """
        Using the instrument protocol, fetch a parameter from the instrument
        @param content A list of parameters to fetch
        @retval A dictionary with the parameter and value of the requested
            parameter
        """

    def op_set_params(self, content, headers, msg):
        """
        Using the instrument protocol, set a parameter on the instrument
        @param content A dictionary with the parameters and values to set
        @retval A small dict of parameter and value on success, empty dict on
            failure
        """

    def op_execute(self, content, headers, msg):
        """
        Using the instrument protocol, execute the requested command
        @param command A list where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval Result code of some sort
        """

    def op_configure_driver(self, content, headers, msg):
        """
        This method takes a dict of settings that the driver understands as
        configuration of the driver itself (ie 'target_ip', 'port', etc.). This
        is the bootstrap information for the driver and includes enough
        information for the driver to start communicating with the instrument.
        @param content A dict with parameters for the driver
        """

    def op_disconnect(self, content, headers, msg):
        """
        Disconnect from the instrument
        @param none
        """

class InstrumentDriverClient(ProcessClient):
    """
    The base class for the instrument driver client interface. This interface
    is designed to be used by the instrument agent to work with the driver.
    """

    @defer.inlineCallbacks
    def fetch_params(self, param_list):
        """
        Using the instrument protocol, fetch a parameter from the instrument
        @param param_list A list or tuple of parameters to fetch
        @retval A dictionary with the parameter and value of the requested
            parameter
        """
                
        assert(isinstance(param_list, (list, tuple)))
        (content, headers, message) = yield self.rpc_send('fetch_params',
                                                          param_list)
        assert(isinstance(content, dict))
        defer.returnValue(content)


    @defer.inlineCallbacks
    def set_params(self, param_dict):
        """
        Using the instrument protocol, set a parameter on the instrument
        @param param_dict A dictionary with the parameters and values to set
        @retval A small dict of parameter and value on success, empty dict on
            failure
        """
        
        assert(isinstance(param_dict, dict))
        (content, headers, message) = yield self.rpc_send('set_params',
                                                          param_dict)
        assert(isinstance(content, dict))
        defer.returnValue(content)



    @defer.inlineCallbacks
    def execute(self, command):
        """
        Using the instrument protocol, execute the requested command
        @param command An ordered list of lists where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval Result code of some sort
        """
        log.debug("Driver client executing command: %s", command)
        assert(isinstance(command, (list, tuple))), "Bad Driver client execute type"
        (content, headers, message) = yield self.rpc_send('execute',
                                                          command)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_status(self, arg):
        """
        Using the instrument protocol, gather status from the instrument
        @param arg The argument needed for gathering status
        @retval Result message of some sort
        """
        (content, headers, message) = yield self.rpc_send('get_status', arg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def configure_driver(self, config_vals):
        """
        This method takes a dict of settings that the driver understands as
        configuration of the driver itself (ie 'target_ip', 'port', etc.). This
        is the bootstrap information for the driver and includes enough
        information for the driver to start communicating with the instrument.
        @param config_vals A dict with parameters for the driver
        """
        assert(isinstance(config_vals, dict))
        (content, headers, message) = yield self.rpc_send('configure_driver',
                                                          config_vals)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def initialize(self, arg):
        """
        Disconnect from the instrument
        @param none
        @retval Result code of some sort
        """
        #assert(isinstance(command, dict))
        log.debug("DHE: in initialize!")
        (content, headers, message) = yield self.rpc_send('initialize',
                                                          arg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def disconnect(self, command):
        """
        Disconnect from the instrument
        @param none
        @retval Result code of some sort
        """
        #assert(isinstance(command, dict))
        log.debug("DHE: in IDC disconnect!")
        (content, headers, message) = yield self.rpc_send('disconnect',
                                                          command)
        defer.returnValue(content)

class InstrumentAgent(ResourceAgent):
    """
    The base class for developing Instrument Agents. This defines
    the interface to use for an instrument agent and some boiler plate
    function implementations that may be good enough for most agents.
    Child instrument agents should supply instrument-specific routines such as
    getCapabilities.
    """
    
    
    """
    The software version of the instrument agent.
    """
    version = '0.1'
    
    @classmethod
    def get_version(cls):
        """
        Return the software version of the instrument agent.
        """
        return cls.version
    
    
    def plc_init(self):
        
        ResourceAgent.plc_init(self)
        self.pubsub_client = PubSubClient(proc=self)

        """
        The driver client to communicate with the child driver
        """
        self.driver_client = None
        
        """
        A dictionary of the topics where data is published, indexed by transducer
        name or "Device" for the whole device. Gets set initially by
        subclass, then at runtime by user as needed.
        """
        self.output_topics = None
    
        """
        A dictionary of the topics where events are published, indexed by
        transducer name or "Device" for the whole device. Gets set initially by
        subclass, then at runtime by user as needed.
        """
        self.event_topics = None
    
        """
        A dictionary of the topics where state changes are published, indexed by
        transducer name or "Device" for the whole device. Gets set initially by
        subclass, then at runtime by user as needed.
        """
        self.state_topics = None
    
        """
        A UUID specifying the current transaction. None
        indicates no current transaction.
        """
        self.transaction_id = None
        
        """
        An integer in seconds for how long to wait to acquire a new transaction if
        a value is not explicitly given.
        """
        self.default_transaction_timeout = 10   
        
        """
        An integer in seconds for the maximum allowable timeout to wait for a new transaction.
        """
        self.max_transaction_timeout = 120
    
        """
        An integer in seconds for the maximum time a transaction may be open.
        """
        self.transaction_expire_timeout = 300
    
        """
        A finite state machine to track and manage agent state according to the general
        instrument state model.
        """
        self.agent_fsm = None
    
        """
        String indicating the source of time being used for the instrument.
        See time_sources list for available values.
        """
        self.time_source = 'TIME_NOT_SPECIFIED'
    
        """
        String describing how the device is connected to the observatory.
        See connection_methods list for available values.
        """
        self.connection_method = 'CONNECTION_NOT_SPECIFIED'
        
        """
        Buffer to hold instrument data for periodic transmission.
        """
        #TODO driver integration. I think this is a list of strings.
        self.data_buffer = []
    
        """
        List of current alarm conditions. Tuple of (ID,description).
        """
        self.alarms = []
        
        """
        Dictionary of time status values.
        """
        self.time_status = {
            'Uncertainty': None,
            'Peers' : None
        }


        
    @defer.inlineCallbacks
    def _register_publisher(self):
        publisher = PublisherResource.create("IA publisher", self,
            self.output_topics.values() + self.event_topics.values() + self.state_topics.values(),
            'DataObject')
        publisher = yield self.pubsub_client.define_publisher(publisher)

    def _is_child_process(self, name):
        """
        Determine if a process with the given name is a child process
        @param name The name to test for subprocess-ness
        @retval True if the name matches a child process name, False otherwise
        """
        log.debug("__is_child_process looking for process '%s' in %s",
                  name, self.child_procs)
        found = False
        for proc in self.child_procs:
            if proc.proc_name == name:
                found = True
                break
        return found



    ############################################################################
    #   Transaction Management
    ############################################################################


    @defer.inlineCallbacks
    def op_start_transaction(self,content,headers,msg):
        """
        Begin an exclusive transaction with the agent.
        @param content An integer specifying the time to wait in seconds for the
            transaction.
        @retval Transaction ID UUID string.
        """
        
        result = yield self._start_transaction(content)                
        yield self.reply_ok(msg,result)
        
    
    def _start_transaction(self,timeout):
        """
        Begin an exclusive transaction with the agent.
        @param timeout An integer specifying time to wait in seconds for the transaction.
        @retval Transaction ID UUID string.
        """
        
        assert(isinstance(timeout,int)), 'Expected an integer timeout.'
        
        result = {'success':None,'transaction_id':None}
        
        if timeout < 0:
            timeout = self.default_transaction_timeout
        
        if timeout > self.max_transaction_timeout:
            timeout = self.max_transaction_timeout
            
        if timeout == 0:
            if self.transaction_id == None:
                self.transaction_id = str(uuid4())
                result['success'] = ['OK']
                result['transaction_id']=self.transaction_id
            else:
                result['success'] = errors['LOCKED_RESOURCE']
                
        else:
            #TODO replay this with callback/timeout logic as necessary
            if self.transaction_id == None:
                self.transaction_id = str(uuid4())
                result['success'] = ['OK']
                result['transaction_id']=self.transaction_id
            else:
                result['success'] = errors['LOCKED_RESOURCE']
        
        return result
        
        
        
    
    @defer.inlineCallbacks
    def op_end_transaction(self,content,headers,msg):
        """
        End the current transaction.
        @param tid A uuid specifying the current transaction to end.
        """        

        result = self._end_transaction(content)
        yield self.reply_ok(msg,result)
                
    

    def _end_transaction(self,tid):
        """
        End the current transaction.
        @param tid A uuid specifying the current transaction to end.
        """        
        
        assert(isinstance(tid,str)), 'Expected a str transaction ID.'

        result = {'success':None}

        if tid == self.transaction_id:
            self.transaction_id = None
            result['success'] = ['OK']
        elif self.transaction_id == None:
            result['success'] = errors['RESOURCE_NOT_LOCKED']
        else:
            result['success'] = errors['LOCKED_RESOURCE']

            
        return result
        

    
    def _verify_transaction(self,tid,optype):
        """
        Verify the passed transaction ID is currently open, or open an implicit transaction.
        @param tid 'create' to create an implicit transaction, 'none' to perform the operation without
            a transaction, or a UUID to test against the current transaction ID.
        @param optype 'get' 'set' or 'execute'
        @retval True if the transaction is valid or if one was successfully created, False otherwise.
        """


        assert(isinstance(tid,str)), 'Expected transaction ID str.'
        assert(isinstance(optype,str)), 'Expected str optype.'


        # Try to start an implicit transaction if tid is 'create'
        if tid == 'create':
            result = self._start_transaction(self.default_transaction_timeout)
            success = result['success']
            if success[0]=='OK':
                return True
            else:
                return False
        
        # Allow only gets without a current or created transaction.
        if tid == 'none' and self.transaction_id == None and optype == 'get':
            return True
        
        # Otherwise, the given ID must match the outstanding one
        if tid == self.transaction_id:
            return True
        
        return False
    
    
    ############################################################################
    #   Observatory Facing Interface
    ############################################################################
    


    @defer.inlineCallbacks
    def op_execute_observatory(self, content, headers, msg):
        """
        Execute infrastructure commands related to the Instrument Agent
        instance. This includes commands for messaging, resource management
        processes, etc.
        @param content A dict {'command':[command,arg, ,arg],'transaction_id':transaction_id)}
        @retval ACK message containing a dict
            {'success':success,'result':command-specific,'transaction_id':transaction_id}.
        """
        
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('command')), 'Expected a command.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        cmd = content['command']
        tid = content['transaction_id']

        assert(isinstance(cmd,(tuple,list))), 'Expected a command list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'
    
        reply = {'success':None,'result':None,'transaction_id':None}
    
        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'execute')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return
          
                                                
        reply['transaction_id'] = self.transaction_id    
        
                     
        if  cmd[0] == 'CI_CMD_STATE_TRANSITION':
            #output = self.agent_fsm.state_transition(cmd[1])
            if cmd[1] not in ci_transition_list:
                reply['success'] = errors['UNKNOWN_TRANSITION']
            else:
                # output = self.agent_fsm.state_transition(cmd[1])
                # TODO FSM integration
                reply['success'] = ['OK']
        elif cmd[0] == 'CI_CMD_TRANSMIT_DATA':
            reply['success'] = errors['NOT_IMPLEMENTED']
        else:
            reply['success'] = errors['UNKNOWN_COMMAND']
        

        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
        
        yield self.reply_ok(msg,reply)
            
        
    @defer.inlineCallbacks
    def op_get_observatory(self, content, headers, msg):
        """
        Get data from the cyberinfrastructure side of the agent (registry info,
        topic locations, messaging parameters, process parameters, etc.)
        @param content A dict {'params':[param_arg, ,param_arg],'transaction_id':transaction_id}.
        @retval A reply message containing a dict
            {'success':success,'result':{param_arg:(success,val),...,param_arg:(success,val)},
            'transaction_id':transaction_id)
        """

        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,(tuple,list))), 'Expected a parameter list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'

        reply = {'success':None,'result':None,'transaction_id':None}


        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        # Set up the transaction
        result = yield self._verify_transaction(tid,'get')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id
                            
        result = {}                    
        get_errors = False
                
        # Add each observatory parameter given in params list.
        for arg in params:
            if arg not in ci_param_list and arg != 'all':
                result[arg] = (errors['INVALID_PARAMETER'], None)
                get_errors = True                
                continue
            if arg == 'CI_PARAM_DATA_TOPICS' or arg=='all':                            
                if self.output_topics == None:
                    result['CI_PARAM_DATA_TOPICS'] = (['OK'],None)
                else:
                    # TODO
                    # result['CI_PARAM_DATA_TOPICS'] = self.output_topics.encode()
                    pass
                
            if arg == 'CI_PARAM_EVENT_TOPICS' or arg=='all':
                if self.output_topics == None:
                    result['CI_PARAM_EVENT_TOPICS'] = (['OK'],None)
                else:
                    # TODO
                    # result['CI_PARAM_EVENT_TOPICS'] = self.event_topics.encode()
                    pass
                
            if arg == 'CI_PARAM_STATE_TOPICS' or arg=='all':
                if self.output_topics == None:
                    result['CI_PARAM_STATE_TOPICS'] = (['OK'],None)
                else:
                    # TODO
                    # result['CI_PARAM_STATE_TOPICS'] = self.state_topics.encode()
                    pass
                
            if arg == 'CI_PARAM_DRIVER_ADDRESS' or arg=='all':
                if self.driver_client:
                    result['CI_PARAM_DRIVER_ADDRESS'] = (['OK'],str(self.driver_client.target))
                else:
                    get_errors = True
                    result['CI_PARAM_DRIVER_ADDRESS'] = (errors['INVALID_DRIVER'],None)
                    
            if arg == 'CI_PARAM_RESOURCE_ID' or arg=='all':
                # TODO: how do we access this?
                result['CI_PARAM_RESOURCE_ID'] = (['OK'],None)
            
            if arg == 'CI_PARAM_TIME_SOURCE' or arg=='all':
                result['CI_PARAM_TIME_SOURCE'] = (['OK'],self.time_source)
                
            if arg == 'CI_PARAM_CONNECTION_METHOD' or arg=='all':
                result['CI_PARAM_CONNECTION_METHOD'] = (['OK'],self.connection_method)
                
            if arg == 'CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT' or arg=='all':
                result['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'] = (['OK'],self.default_transaction_timeout)
                
            if arg == 'CI_PARAM_MAX_TRANSACTION_TIMEOUT' or arg=='all':
                result['CI_PARAM_MAX_TRANSACTION_TIMEOUT'] = (['OK'],self.max_transaction_timeout)
                
            if arg == 'CI_PARAM_TRANSACTION_EXPIRE_TIMEOUT' or arg=='all':
                result['CI_PARAM_TRANSACTION_EXPIRE_TIMEOUT'] = (['OK'],self.transaction_expire_timeout)
                
        if get_errors:
            success = errors['GET_OBSERVATORY_ERR']
        else:
            success = ['OK']
            
        reply['success'] = success
        reply['result'] = result
        
        # Do the work.
        #response = {}
        ## get data somewhere, or just punt this lower in the class hierarchy
        #if (ci_param_list[driver_address] in content):
        #    response[ci_param_list[driver_address]] = str(self.driver_client.target)
        #
        #if (ci_param_list['DataTopics'] in content):
        #    response[ci_param_list['DataTopics']] = {}
        #    for i in self.output_topics.keys():
        #        response[ci_param_list['DataTopics']][i] = self.output_topics[i].encode()
        #if (ci_param_list['StateTopics'] in content):
        #    response[ci_param_list['StateTopics']] = {}
        #    for i in self.state_topics.keys():
        #        response[ci_param_list['StateTopics']][i] = self.state_topics[i].encode()
        #if (ci_param_list['EventTopics'] in content):
        #    response[ci_param_list['EventTopics']] = {}
        #    for i in self.event_topics.keys():
        #        response[ci_param_list['EventTopics']][i] = self.event_topics[i].encode()

        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)
        


    @defer.inlineCallbacks
    def op_set_observatory(self, content, headers, msg):
        """
        Set parameters related to the infrastructure side of the agent
        (registration information, location, network addresses, etc.)
        @param content A dict {'params':{param_arg:val,..., param_arg:val},
            'transaction_id':transaction_id}.
        @retval Reply message with dict
            {'success':success,'result':{param_arg:success,...,param_arg:success},'transaction_id':transaction_id}.
        """
        
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,dict)), 'Expected a parameter dict.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'
        
        reply = {'success':None,'result':None,'transaction_id':None}
        
        
        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return
        

        # Set up the transaction
        result = yield self._verify_transaction(tid,'set')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return
                    
        reply['transaction_id'] = self.transaction_id
        
        
        result = {}
        set_errors = False
        
        # Do the work here.
        # Set up the result message.

        # Add each observatory parameter given in params list.
        # Note: it seems like all the current params should be read only by
        # general agent users. 
        for arg in params.keys():
            if arg not in ci_param_list:
                result[arg] = errors['INVALID_PARAMETER']
                set_errors = True
                continue
            
            val = params[arg]
            
            if arg == 'CI_PARAM_DATA_TOPICS':
                result[arg] = errors['NOT_IMPLEMENTED']
                set_errors = True
                
            elif arg == 'CI_PARAM_EVENT_TOPICS':
                result[arg] = errors['NOT_IMPLEMENTED']
                set_errors = True
            
            elif arg == 'CI_PARAM_STATE_TOPICS':
                result[arg] = errors['NOT_IMPLEMENTED']
                set_errors = True
            
            elif arg == 'CI_PARAM_DRIVER_ADDRESS':
                result[arg] = errors['NOT_IMPLEMENTED']
                set_errors = True
            
            elif arg == 'CI_PARAM_RESOURCE_ID':
                result[arg] = errors['NOT_IMPLEMENTED']
                set_errors = True
            
            elif arg == 'CI_PARAM_TIME_SOURCE':
                if val in time_sources:
                    if val != self.time_source:
                        self.time_source = val
                        # Logic here when new time source set.
                        # And test for successful switch.
                        success = ['OK']
                    else:
                        success = ['OK']
                else:
                    set_errors = True
                    success = errors['INVALID_PARAM_VALUE']
                result[arg] = success
                
            elif arg == 'CI_PARAM_CONNECTION_METHOD':
                if val in connection_methods:
                    if val != self.connection_method:
                        self.connection_method = val
                        # Logic here when new connection method set.
                        # And test for successful switch.
                        success = ['OK']
                    else:
                        success = ['OK']
                else:
                    set_errors = True
                    success = errors['INVALID_PARAM_VALUE']
                result[arg] = success
                
            elif arg == 'CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT':
                if isinstance(val,int) and val >= 0:
                    self.default_transaction_timeout = val
                    success = ['OK']
                    if self.max_transaction_timeout < val:
                        self.max_transaction_timeout = val
                        result['CI_PARAM_MAX_TRANSACTION_TIMEOUT'] = ['OK']
                else:
                    set_errors = True
                    success = errors['INVALID_PARAM_VALUE']
                result[arg] = success
                
            elif arg == 'CI_PARAM_MAX_TRANSACTION_TIMEOUT':
                if isinstance(val,int) and val >= 0:
                    self.max_transaction_timeout = val
                    success = ['OK']
                    if self.default_transaction_timeout > val:
                        self.default_transaction_timeout = val
                        result['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'] = ['OK']
                else:
                    set_errors = True
                    success = errors['INVALID_PARAM_VALUE']
                result[arg] = success

            elif arg == 'CI_PARAM_TRANSACTION_EXPIRE_TIMEOUT':
                if isinstance(val,int) and val > self.min_transaction_expire_timeout:
                    self.transaction_expire_timeout = val
                    success = ['OK']
                else:
                    set_errors = True
                    success = errors['INVALID_PARAM_VALUE']
                result[arg] = success



        if set_errors:
            success = errors['SET_OBSERVATORY_ERR']
        else:
            success = ['OK']
            
        reply['success'] = success
        reply['result'] = result
            
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)
 
 
    @defer.inlineCallbacks
    def op_get_observatory_metadata(self,content,headers,msg):
        """
        Retrieve metadata about the observatory configuration parameters.
        @param content A dict
            {'params':[(param_arg,meta_arg),...,(param_arg,meta_arg)],'transaction_id':transaction_id}
        @retval A reply message with a dict {'success':success,'result':{param_arg:{meta_arg):(success,val),...,
            meta_arg:(success,val)},...param_arg:{meta_arg:(success,val),...,meta_arg:(success,val)}},'transaction_id':transaction_id}.
        """
        
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,(tuple,list))), 'Expected a parameter list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'

        reply = {'success':None,'result':None,'transaction_id':None}

        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'get')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id

                    
        get_errors = False
        result = {}
        

                
                                
        # Do the work here.
        # Set up the result message.
        for (param_arg,meta_arg) in params:
            
            
            if param_arg == 'all' and meta_arg == 'all':
                for param_key in ci_param_list:
                    if not result.has_key(param_key):
                        result[param_key] = {}
                    if param_key not in ci_param_metadata.keys():
                        result[param_key].update({meta_arg:(errors['NO_PARAM_METADATA'],None)})
                        get_errors = True
                    else:
                        for meta_key in ci_param_metadata[param_key]:
                            val = ci_param_metadata[param_key][meta_key]
                            result[param_key].update({meta_key:(['OK'],val)})
                                                            
            
            elif param_arg == 'all' and meta_arg != 'all':
                for param_key in ci_param_list:
                    if not result.has_key(param_key):
                        result[param_key] = {}
                    if param_key not in ci_param_metadata.keys():
                        result[param_key].update({meta_arg:(errors['NO_PARAM_METADATA'],None)})
                        get_errors = True
                    elif meta_arg not in metadata_list:
                        result[param_key].update({meta_arg:(errors['INVALID_METADATA'],None)})
                        get_errors = True
                    else:
                        try:
                            val = ci_param_metadata[param_key][meta_arg]
                        except:
                            result[param_key].update({meta_arg:(errors['INVALID_METADATA'],None)})
                            get_errors = True
                        else:
                            result[param_key].update({meta_arg:(['OK'],val)})
                            
                                        
            elif param_arg != 'all' and meta_arg == 'all':
                if not result.has_key(param_arg):
                    result[param_arg] = {}
                if param_arg not in ci_param_list:
                    result[param_arg].update({meta_arg:(errors['INVALID_PARAMETER'],None)})
                    get_errors = True
                elif param_arg not in ci_param_metadata.keys():
                    result[param_arg].update({meta_arg:(errors['NO_PARAM_METADATA'],None)})
                    get_errors = True
                else:
                    for meta_key in ci_param_metadata[param_arg].keys():
                        val = ci_param_metadata[param_arg][meta_key]
                        result[param_arg].update({meta_key:(['OK'],val)})
                
            else:
                if not result.has_key(param_arg):
                    result[param_arg] = {}
                if param_arg not in ci_param_list:
                    result[param_arg].update({meta_arg:(errors['INVALID_PARAMETER'],None)})
                    get_errors = True
                elif param_arg not in ci_param_metadata.keys():
                    result[param_arg].update({meta_arg:(errors['NO_PARAM_METADATA'],None)})
                    get_errors = True
                else:
                    try:
                        val = ci_param_metadata[param_arg][meta_arg]
                    except:
                        result[param_arg].update({meta_arg:(errors['INVALID_METADATA'],None)})    
                    else:
                        result[param_arg].update({meta_arg:(['OK'],val)})
        
        
        if get_errors:
            success = errors['GET_OBSERVATORY_ERR']
        else:
            success = ['OK']
            
        reply['success'] = success
        reply['result'] = result
        
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)


    @defer.inlineCallbacks
    def op_get_observatory_status(self,content,headers,msg):
        """
        Retrieve the observatory status values, including lifecycle state and other
        dynamic observatory status values indexed by status keys.
        @param content A dict {'params':[status_arg,...,status_arg],'transaction_id':transaction_id}.
        @retval Reply message with a dict
            {'success':success,'result':{status_arg:(success,val),..., status_arg:(success,val)},
            'transaction_id':transaction_id}
        """
        
        
        
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,(tuple,list))), 'Expected a parameter list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'

        reply = {'success':None,'result':None,'transaction_id':None}


        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'get')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id

                    
        get_errors = False
        result = {}
        
        # Do the work here.
        # Set up the result message.
        for arg in params:
            if arg not in ci_status_list and arg != 'all':
                result[arg] = (errors['INVALID_STATUS'],None)
                get_errors = True
                continue
            
            if arg == 'CI_STATUS_AGENT_STATE' or arg == 'all':
                # TODO FSM integration.
                #result['CI_STATUS_AGENT_STATE'] = (['OK'],self.agent_fsm.get_state())
                result['CI_STATUS_AGENT_STATE'] = (['OK'],'CI_STATE_UNKNOWN')
            if arg == 'CI_STATUS_CHANNEL_NAMES' or arg == 'all':
                # TODO driver integration.
                #dvr_msg_content = {'params':[('instrument','channel_names')]}
                #dvr_result = yield self.driver_client.rpc_send('get',dvr_msg_content)
                #dvr_success = dvr_result['success']
                #dvr_val = dvr_result['params'][('instrument','channel_names')]
                dvr_val = [['OK'],['CHAN_1','CHAN_2','CHAN_3']]
                result['CI_STATUS_CHANNEL_NAMES'] = (dvr_val[0],dvr_val[1])                
                if dvr_val[0][0] != 'OK':
                    get_errors = True
            if arg == 'CI_STATUS_INSTRUMENT_CONNECTION_STATE' or arg == 'all':
                #TODO driver integration.
                #dvr_msg_content = {'params':[('instrument','connection_status')]}
                #dvr_result = yield self.driver_client.rpc_send('get_status',dvr_msg_content)
                #dvr_success = dvr_result['success']
                #dvr_val = dvr_result['params'][('instrument','connection_status')]
                dvr_val=[['OK'],'DRIVER_CONNECTION_STATE']
                result['CI_STATUS_INSTRUMENT_CONNECTION_STATE'] = (dvr_val[0],dvr_val[1])                    
                if dvr_val[0][0] != 'OK':
                    get_errors = True
            if arg == 'CI_STATUS_ALARMS' or arg == 'all':
                result['CI_STATUS_ALARMS'] = (['OK'],self.alarms)
            if arg == 'CI_STATUS_TIME_STATUS' or arg == 'all':
                result['CI_STATUS_TIME_STATUS'] = (['OK'],self.time_status)
            if arg == 'CI_STATUS_BUFFER_SIZE' or arg == 'all':
                result['CI_STATUS_BUFFER_SIZE'] = (['OK'],self._get_buffer_size())
            if arg == 'CI_STATUS_AGENT_VERSION' or arg == 'all':
                result['CI_STATUS_AGENT_VERSION'] = (['OK'],self.get_version())
            if arg == 'CI_STATUS_DRIVER_VERSION' or arg == 'all':
                #TODO driver integration.
                #version = yield self.driver_client.get_version()
                version = '0.1'
                result['CI_STATUS_DRIVER_VERSION'] = (['OK'],version)
                
                
        if get_errors:
            success = errors['GET_OBSERVATORY_ERR']
        else:
            success = ['OK']
            
        reply['success'] = success
        reply['result'] = result
        
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)
        


    @defer.inlineCallbacks
    def op_get_capabilities(self,content,headers,msg):
        """
        Retrieve the agent capabilities, including observatory and device values,
        both common and specific to the agent / device.
        @param content A dict {'params':[cap_arg,...,cap_arg],'transaction_id':transaction_id} 
        @retval Reply message with a dict {'success':success,'result':{cap_arg:(success,[cap_val,...,cap_val]),...,
            cap_arg:(success,[cap_val,...,cap_val])}, 'transaction_id':transaction_id}
        """
        
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,(tuple,list))), 'Expected a parameter list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'

        reply = {'success':None,'result':None,'transaction_id':None}

        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'get')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id

                    
        get_errors = False
        result = {}
        
        # Do the work here.
        # Set up the result message.
        for arg in params:
            if arg not in capabilities_list or arg != 'all':
                result[arg] = (errors['INVALID_CAP_PARAM'],None)
                
            elif arg == 'CAP_OBSERVATORY_COMMANDS' or arg == 'all':
                result['CAP_OBSERVATORY_COMMANDS'] = (['OK'],ci_command_list)
                
            elif arg == 'CAP_OBSERVATORY_PARAMS' or arg == 'all':
                result['CAP_OBSERVATORY_PARAMS'] = (['OK'],ci_param_list)
                
            elif arg == 'CAP_OBSERVATORY_STATUSES' or arg == 'all':
                result['CAP_OBSERVATORY_STATUSES'] = (['OK'],ci_status_list)
                
            elif arg == 'CAP_METADATA' or arg == 'all':
                result['CAP_METADATA'] = (['OK'],metadata_list)
                
            elif arg == 'CAP_DEVICE_COMMANDS' or arg == 'all':
                dvr_content = {'params':'CAP_DEVICE_COMMANDS'}
                dvr_result = yield self.driver_client.rpc_send('get_capabilities',dvr_content)
                dvr_success = dvr_result[0]
                dvr_val = dvr_result[1]['CAP_DEVICE_COMMANDS']
                result['CAP_DEVICE_COMMANDS'] = (dvr_val[0],dvr_val[1])
                if dvr_success[0] != 'OK':
                    get_errors = True
                
            elif arg == 'CAP_DEVICE_PARAMS' or arg == 'all':
                dvr_content = {'params':'CAP_DEVICE_PARAMS'}
                dvr_result = yield self.driver_client.rpc_send('get_capabilities',dvr_content)
                dvr_success = dvr_result[0]
                dvr_val = dvr_result[1]['CAP_DEVICE_PARAMS']
                result['CAP_DEVICE_PARAMS'] = (dvr_val[0],dvr_val[1])
                if dvr_success[0] != 'OK':
                    get_errors = True
                
            elif arg == 'CAP_DEVICE_STATUSES' or arg == 'all':
                dvr_content = {'params':'CAP_DEVICE_STATUSES'}
                dvr_result = yield self.driver_client.rpc_send('get_capabilities',dvr_content)
                dvr_success = dvr_result[0]
                dvr_val = dvr_result[1]['CAP_DEVICE_STATUSES']
                result['CAP_DEVICE_STATUSES'] = (dvr_val[0],dvr_val[1])
                if dvr_success[0] != 'OK':
                    get_errors = True
 
        
        if get_errors:
            success = errors['GET_OBSERVATORY_ERR']
        else:
            success = ['OK']
            
        reply['success'] = success
        reply['result'] = result
        
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)
        


    ############################################################################
    #   Instrument Facing Interface
    ############################################################################


    @defer.inlineCallbacks
    def op_execute_device(self, content, headers, msg):
        """
        Execute a command on the device fronted by the agent. Commands may be
        common or specific to the device, with specific commands known through
        knowledge of the device or a previous get_capabilities query.
        @param content A dict
            {'channels':[chan_arg,...,chan_arg],'command':[command,arg,...,argN]),'transaction_id':transaction_id)
       @retval A reply message with a dict
            {'success':success,'result':{chan_arg:(success,command_specific_values),...,chan_arg:(success,command_specific_values)},
            'transaction_id':transaction_id}. 
        """

        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('channels')), 'Expected channels.'
        assert(content.has_key('command')), 'Expected command.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        channels = content['channels']
        command = content['command']
        tid = content['transaction_id']
        
        assert(isinstance(channels,(tuple,list))), 'Expected a channels list or tuple.'
        assert(isinstance(command,(tuple,list))), 'Expected a command list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'

        reply = {'success':None,'result':None,'transaction_id':None}

        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'execute')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id
                    
        # Do the work here.
        # Set up the result message.
        dvr_content = {'command':command,'channels':channels}
        dvr_result = yield self.driver_client.rpc_send('execute',dvr_content)
        
        reply['success'] = dvr_result['success']
        reply['result'] = dvr_result['result']
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)


    @defer.inlineCallbacks
    def op_get_device(self, content, headers, msg):
        """
        Get configuration parameters from the instrument. 
        @param content A dict {'params':[(chan_arg,param_arg),...,(chan_arg,param_arg)],'transaction_id':transaction_id}
        @retval A reply message with a dict
            {'success':success,'result':{(chan_arg,param_arg):(success,val),...,(chan_arg,param_arg):(success,val)},
            'transaction_id':transaction_id}
        """
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,(tuple,list))), 'Expected a parameter list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'
        
        reply = {'success':None,'result':None,'transaction_id':None}

        

        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'get')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id
                    
        # Do the work here.
        # Set up the result message.
        dvr_content = {'params':params}
        dvr_result = yield self.driver_client.rpc_send('get',dvr_content)
        
        reply['success'] = dvr_result['success']
        reply['result'] = dvr_result['result']
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)



    @defer.inlineCallbacks
    def op_set_device(self, content, headers, msg):
        """
        Set parameters to the instrument side of of the agent. 
        @param content A dict {'params':{(chan_arg,param_arg):val,...,(chan_arg,param_arg):val},
            'transaction_id':transaction_id}.
        @retval Reply message with a dict
            {'success':success,'result':{(chan_arg,param_arg):success,...,chan_arg,param_arg):success},
            'transaction_id':transaction_id}.
        """
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,dict)), 'Expected a parameter dict.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'
        
        reply = {'success':None,'result':None,'transaction_id':None}

        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'set')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id
                    
        # Do the work here.
        # Set up the result message.
        dvr_content = {'params':params}
        dvr_result = yield self.driver_client.rpc_send('set',dvr_content)
        
        reply['success'] = dvr_result['success']
        reply['result'] = dvr_result['result']
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)



    @defer.inlineCallbacks
    def op_get_device_metadata(self, content, headers, msg):
        """
        Retrieve metadata for the device, its transducers and parameters.
        @param content A dict {'params':[(chan_arg,param_arg,meta_arg),...,(chan_arg,param_arg,meta_arg)],
            'transaction_id':transaction_id}
        @retval Reply message with a dict
            {'success':success,'result':{(chan_arg,param_arg,meta_arg):(success,val),...,
            chan_arg,param_arg,meta_arg):(success,val)}, 'transaction_id':transaction_id}.
        """
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,(tuple,list))), 'Expected a parameter list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'
        
        reply = {'success':None,'result':None,'transaction_id':None}

        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'get')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id
                    
        # Do the work here.
        # Set up the result message.
        dvr_content = {'params':params}
        dvr_result = yield self.driver_client.rpc_send('get_metadata',dvr_content)
        
        reply['success'] = dvr_result['success']
        reply['result'] = dvr_result['result']
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)



    @defer.inlineCallbacks
    def op_get_device_status(self, content, headers, msg):
        """
        Obtain the status of an instrument. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param content A dict {'params':[(chan_arg,status_arg),...,chan_arg,status_arg)],
            'transaction_id':transaction_id}.
        @retval A reply message with a dict
            {'success':success,'result':{(chan_arg,status_arg):(success,val),...,
            chan_arg,status_arg):(success,val)}, 'transaction_id':transaction_id}.
        """
        
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('params')), 'Expected params.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        params = content['params']
        tid = content['transaction_id']
        
        assert(isinstance(params,(tuple,list))), 'Expected a parameter list or tuple.'
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'
        
        reply = {'success':None,'result':None,'transaction_id':None}

        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'get')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id
                    
        # Do the work here.
        # Set up the result message.
        dvr_content = {'params':params}
        dvr_result = yield self.driver_client.rpc_send('get_status',dvr_content)
        
        reply['success'] = dvr_result['success']
        reply['result'] = dvr_result['result']
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)


    @defer.inlineCallbacks
    def op_execute_device_direct(self,content,headers,msg):
        """
        Execute untranslated byte data commands on the device.
        Must be in direct access mode and possess the correct transaction_id key
        for the direct access session.
        @param content A dict {'bytes':block_of_data,'transaction_id':transaction_id}
        @retval A dict {'success':success,'result':block_of_data}.
        """
        
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('bytes')), 'Expected bytes.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        bytes = content['bytes']
        tid = content['transaction_id']

        # expect a byte string?
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'

        
        reply = {'success':None,'result':None,'transaction_id':None}

        if tid != 'create' and tid != 'none' and len(tid) != 36:
            reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return


        # Set up the transaction
        result = yield self._verify_transaction(tid,'execute')
        if not result:
            if tid == 'none':
                reply['success'] = errors['TRANSACTION_REQUIRED']        
            elif tid=='create':
                reply['success'] = errors['LOCKED_RESOURCE']
            else:
                reply['success'] = errors['INVALID_TRANSACTION_ID']
            yield self.reply_ok(msg,reply)
            return

        reply['transaction_id'] = self.transaction_id
                    
        # Do the work here.
        # Set up the result message.
        dvr_content = {'bytes':bytes}
        dvr_result = yield self.driver_client.rpc_send('execute_direct',dvr_content)
        
        reply['success'] = dvr_result['success']
        reply['result'] = dvr_result['result']
        
        
        # End implicit transactions.
        if tid == 'create':
            self._end_transaction(self.transaction_id)
                    
        yield self.reply_ok(msg,reply)
            




    ############################################################################
    #   Publishing Methods
    ############################################################################



                
    @defer.inlineCallbacks
    def op_publish(self, content, headers, msg):
        """
        Collect data from a subprocess (usually the driver) to publish to the
        correct topic, specific to the hardware device, not the agent.
        @param content A dict including: a Type string of "StateChange",
          "ConfigChange", "Error", or "Data", and a Value string with the
          data or message that is to be published. Must also have "Transducer"
          to specify the transducer doing the chagne.
        """
        assert isinstance(content, dict), "InstrumentAgent op_publish argument error"
        log.debug("Agent is publishing with sender: %s, child_procs: %s, content: %s",
                  headers["sender-name"], self.child_procs, content)
        if (self._is_child_process(headers["sender-name"])):
            if (content["Type"] == publish_msg_type["Data"]):
                yield self.pubsub_client.publish(self,
                            self.output_topics[content["Transducer"]].reference(),
                            content["Value"])
            elif ((content["Type"] == publish_msg_type["Error"])
                or (content["Value"] == "ConfigChange")):
                yield self.pubsub_client.publish(self,
                            self.event_topics[content["Transducer"]].reference(),
                            content["Value"])
            elif (content["Type"] == publish_msg_type["StateChange"]):
                yield self.pubsub_client.publish(self,
                            self.state_topics[content["Transducer"]].reference(),
                            content["Value"])
        else:
            # Really should be handled better...what if there isnt a reply
            # expected?
            yield self.reply_err(msg,
                                 "publish invoked from non-child process")
        # return something...like maybe result?
    
    @defer.inlineCallbacks
    def _self_publish(self, type, value):
        """
        Publish a message from the instrument agent to one of the agent
        pubsub topics. Possibly an event or a state change. Probably not data
        @param type The type of information to publish (should be "Error",
            "StateChange", "ConfigChange", "Event")
        @todo Actually write a test case for this!
        """
        assert ((type == publish_msg_type["Error"]) or \
            (type == publish_msg_type["Event"]) or \
        (type == publish_msg_type["StateChange"]) or \
        (type == publish_msg_type["ConfigChange"])), "Bad IA publish type"
        
        if (type == publish_msg_type["Error"]) or \
            (type == publish_msg_type["Event"]) or \
            (type == publish_msg_type["ConfigChange"]):
                yield self.pubsub_client.publish(self.sup,
                            self.event_topics["Agent"].reference(),value)
        if (type == publish_msg_type["StateChange"]):
                yield self.pubsub_client.publish(self.sup,
                            self.state_topics["Agent"].reference(),value)

        
    ############################################################################
    #   Other.
    ############################################################################
        
    def _get_buffer_size(self):
        """
        Return the total size in characters of the data buffer.
        """
        return sum(map(lambda x: len(x),self.data_buffer))
        





        
class InstrumentAgentClient(ResourceAgentClient):
    """
    The base class for an Instrument Agent Client. It is a service
    that allows for RPC messaging
    """
    
    ############################################################################
    #   Transaction Management.
    ############################################################################

    @defer.inlineCallbacks
    def start_transaction(self,timeout):
        """
        Begin an exclusive transaction with the agent.
        @param timeout An integer specifying the time to wait in seconds for the
            transaction.
        @retval Transaction ID UUID string.
        """
        
        
        assert(timeout==None or isinstance(timeout,int)), 'Expected int or None timeout.'
        (content,headers,message) = yield self.rpc_send('start_transaction',timeout)    
        assert(isinstance(content,dict))
        
        defer.returnValue(content)
        
    
    @defer.inlineCallbacks
    def end_transaction(self,tid):
        """
        End the current transaction.
        @param tid A uuid string specifying the current transaction to end.        
        """
        assert(isinstance(tid,str)), 'Expected a transaction_id str.'
        (content,headers,message) = yield self.rpc_send('end_transaction',tid)
        #yield pu.asleep(1)
        #content = {'success':['OK']}
        assert(isinstance(content,dict))
        defer.returnValue(content)

    ############################################################################
    #   Observatory Facing Interface.
    ############################################################################

    @defer.inlineCallbacks
    def execute_observatory(self,command,transaction_id):
        """
        Execute infrastructure commands related to the Instrument Agent
        instance. This includes commands for messaging, resource management
        processes, etc.
        @param command A command list [command,arg, ,arg].
        @param transaction_id A transaction_id uuid4 or string 'create,' 'none.'
        @retval Reply dict {'success':success,'result':command-specific,'transaction_id':transaction_id}.
        """
        
        assert(isinstance(command,list)), 'Expected a command list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'

        content = {'command':command,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('execute_observatory',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)
        

    @defer.inlineCallbacks
    def get_observatory(self,params,transaction_id='none'):
        """
        Get data from the cyberinfrastructure side of the agent (registry info,
        topic locations, messaging parameters, process parameters, etc.)
        @param params A paramter list [param_arg, ,param_arg].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'
        @retval A reply dict {'success':success,'result':{param_arg:(success,val),...,param_arg:(success,val)},
            'transaction_id':transaction_id)        
        """
        
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_observatory',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)
        
        

    @defer.inlineCallbacks
    def set_observatory(self,params,transaction_id='none'):
        """
        Set parameters related to the infrastructure side of the agent
        (registration information, location, network addresses, etc.)
        @param params A parameter-value dict {'params':{param_arg:val,..., param_arg:val}.
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'        
        @retval Reply dict
            {'success':success,'result':{param_arg:success,...,param_arg:success},'transaction_id':transaction_id}.        
        """
        assert(isinstance(params,dict)), 'Expected a parameter-value dict.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('set_observatory',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_observatory_metadata(self,params,transaction_id='none'):
        """
        Retrieve metadata about the observatory configuration parameters.
        @param params A metadata parameter list [(param_arg,meta_arg),...,(param_arg,meta_arg)].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                
        @retval A reply dict {'success':success,'result':{param_arg:{meta_arg):(success,val),...,
            meta_arg:(success,val)},...param_arg:{meta_arg:(success,val),...,meta_arg:(success,val)}},
            'transaction_id':transaction_id}.
        """
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_observatory_metadata',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_observatory_status(self,params,transaction_id='none'):
        """
        Retrieve the observatory status values, including lifecycle state and other
        dynamic observatory status values indexed by status keys.
        @param params A parameter list [status_arg,...,status_arg].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                        
        @retval Reply dict
            {'success':success,'result':{status_arg:(success,val),..., status_arg:(success,val)},
            'transaction_id':transaction_id}        
        """
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_observatory_status',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_capabilities(self,params,transaction_id='none'):
        """
        Retrieve the agent capabilities, including observatory and device values,
        both common and specific to the agent / device.
        @param params A parameter list [cap_arg,...,cap_arg].        
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                        
        @retval Reply dict {'success':success,'result':{cap_arg:(success,[cap_val,...,cap_val]),...,
            cap_arg:(success,[cap_val,...,cap_val])}, 'transaction_id':transaction_id}
        """
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_capabilities',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)


    ############################################################################
    #   Instrument Facing Interface.
    ############################################################################

    @defer.inlineCallbacks
    def execute_device(self,channels,command,transaction_id='none'):
        """
        Execute a command on the device fronted by the agent. Commands may be
        common or specific to the device, with specific commands known through
        knowledge of the device or a previous get_capabilities query.
        @param channels A channels list [chan_arg,...,chan_arg].
        @param command A command list [command,arg,...,argN]).
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                                
        @retval A reply dict
            {'success':success,'result':{chan_arg:(success,command_specific_values),...,chan_arg:(success,command_specific_values)},
            'transaction_id':transaction_id}. 
        """
        assert(isinstance(channels,list)), 'Expected a channels list.'
        assert(isinstance(command,list)), 'Expected a command list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'channels':params,'command':command,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('execute_device',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_device(self,params,transaction_id='none'):
        """
        Get configuration parameters from the instrument. 
        @param params A parameters list [(chan_arg,param_arg),...,(chan_arg,param_arg)].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                                        
        @retval A reply dict
            {'success':success,'result':{(chan_arg,param_arg):(success,val),...,(chan_arg,param_arg):(success,val)},
            'transaction_id':transaction_id}
        """
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_device',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def set_device(self,params,transaction_id='none'):
        """
        Set parameters to the instrument side of of the agent. 
        @param params A parameter-value dict {(chan_arg,param_arg):val,...,(chan_arg,param_arg):val}.
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                                
        @retval Reply dict
            {'success':success,'result':{(chan_arg,param_arg):success,...,chan_arg,param_arg):success},
            'transaction_id':transaction_id}.
        """
        assert(isinstance(params,dict)), 'Expected a parameter-value dict.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('set_device',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_device_metadata(self,params,transaction_id='none'):
        """
        Retrieve metadata for the device, its transducers and parameters.
        @param params A metadata parameter list [(chan_arg,param_arg,meta_arg),...,(chan_arg,param_arg,meta_arg)].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                                        
        @retval Reply dict
            {'success':success,'result':{(chan_arg,param_arg,meta_arg):(success,val),...,
            chan_arg,param_arg,meta_arg):(success,val)}, 'transaction_id':transaction_id}.
        """
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_device_metadata',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_device_status(self,params,transaction_id='none'):
        """
        Obtain the status of an instrument. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param params A parameter list [(chan_arg,status_arg),...,chan_arg,status_arg)].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                                        
        @retval A reply dict
            {'success':success,'result':{(chan_arg,status_arg):(success,val),...,
            chan_arg,status_arg):(success,val)}, 'transaction_id':transaction_id}.
        """
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_device_status',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def execute_device_direct(self,bytes,transaction_id='none'):
        """
        Execute untranslated byte data commands on the device.
        Must be in direct access mode and possess the correct transaction_id key
        for the direct access session.
        @param bytes An untranslated block of data to send to the device.
        @param transaction_id A transaction ID uuid4 specifying the direct access session.                                               
        @retval A reply dict {'success':success,'result':bytes}.
        """
        assert(bytes), 'Expected command bytes.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'bytes':bytes,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('execute_device_direct',content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    ############################################################################
    #   Publishing interface.
    ############################################################################


    @defer.inlineCallbacks
    def publish(self):
        """
        """
        pass

    ############################################################################
    #   Registration interface.
    ############################################################################


    @defer.inlineCallbacks
    def register_resource(self, instrument_id):
        """
        Register the resource. Since this is a subclass, make the appropriate
        resource description for the registry and pass that into the
        registration call.
        """
        
        """
        ia_instance = InstrumentAgentResourceInstance()
        ci_params = yield self.get_observatory([driver_address])
        ia_instance.driver_process_id = ci_params[driver_address]
        ia_instance.instrument_ref = ResourceReference(
            RegistryIdentity=instrument_id, RegistryBranch='master')
        result = yield ResourceAgentClient.register_resource(self,
                                                             ia_instance)
        defer.returnValue(result)
        """
        pass

# Spawn of the process using the module name
factory = ProcessFactory(InstrumentAgent)
