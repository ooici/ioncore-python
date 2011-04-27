#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/instrument_agent.py
@author Steve Foley
@author Edward Hunter
@brief Instrument Agent, Driver, and Client class definitions
"""


import time
from uuid import uuid4

from twisted.internet import defer, reactor

import ion.util.procutils as pu
import ion.util.ionlog
from ion.core.process.process import Process, ProcessClient, ProcessFactory, ProcessDesc
from ion.services.dm.distribution.events import InfoLoggingEventPublisher
from ion.services.dm.distribution.events import BusinessStateModificationEventPublisher
from ion.services.dm.distribution.events import DataBlockEventPublisher
from ion.agents.instrumentagents.instrument_agent_constants import *


log = ion.util.ionlog.getLogger(__name__)

DEBUG_PRINT = (True,False)[1]

"""
Instrument agent observatory metadata.
"""
ci_param_metadata = {
    
    'CI_PARAM_EVENT_PUBLISHER_ORIGIN' :
        {'META_DATATYPE':'CI_PUBSUB_ORIGIN',
         'META_LAST_CHANGE_TIMESTAMP':(0,0),
         'META_FRIENDLY_NAME':'Event Publisher Origin'},
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





class InstrumentDriver(Process):
    """
    """
    
    def op_execute(self, content, headers, msg):
        """
        """

        
    def op_get(self, content, headers, msg):
        """
        """


    def op_set(self, content, headers, msg):
        """
        """


    def op_get_status(self, content, headers, msg):
        """
        """


    def op_get_state(self,content,headers,msg):
        """
        """


    def op_configure(self, content, headers, msg):
        """
        """


    def op_initialize(self, content, headers, msg):
        """
        """


    def op_connect(self, content, headers, msg):
        """
        """


    def op_disconnect(self, content, headers, msg):
        """
        """




class InstrumentDriverClient(ProcessClient):
    """
    """

    @defer.inlineCallbacks
    def execute(self,channels,command,timeout=None):
        """
        """

        assert(isinstance(channels, (list,tuple))), 'Expected list or tuple channels.'
        assert(isinstance(command, (list,tuple))), 'Expected list or tuple command.'
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'channels':channels,'command':command,'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('execute',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'channels':channels,'command':command,'timeout':None}
            (content, headers, message) = yield self.rpc_send('execute',content_outgoing)            
        
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def get(self, params, timeout=None):
        """
        """
                
        assert(isinstance(params, (list, tuple))), 'Expected a params list or tuple.'                
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'params':params,'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('get',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'params':params,'timeout':None}
            (content, headers, message) = yield self.rpc_send('get',content_outgoing)            
        
        assert(isinstance(content, dict)), 'Expected a reply content dict.'
        defer.returnValue(content)


    @defer.inlineCallbacks
    def set(self, params,timeout=None):
        """
        """
        
        
        assert(isinstance(params, dict)), 'Expected a params dict.'                
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'params':params,'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('set',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'params':params,'timeout':None}
            (content, headers, message) = yield self.rpc_send('set',content_outgoing)
        
        assert(isinstance(content, dict)), 'Expected a reply content dict.'
        defer.returnValue(content)


    @defer.inlineCallbacks
    def get_status(self, params, timeout=None):
        """
        """
        assert(isinstance(params, (list, tuple))), 'Expected a params list or tuple.'        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'params':params,'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('get_status',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'params':params,'timeout':None}
            (content, headers, message) = yield self.rpc_send('get_status',content_outgoing)            
                
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def get_state(self, timeout=None):
        """
        """
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('get_state',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'timeout':None}
            (content, headers, message) = yield self.rpc_send('get_state',content_outgoing)            
                
        assert(isinstance(content,str)), 'Expected a state string reply.'
        defer.returnValue(content)


    @defer.inlineCallbacks
    def configure(self, params, timeout=None):
        """
        """
        assert(isinstance(params, dict)), 'Expected a params dict.'        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'params':params,'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('configure',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'params':params,'timeout':None}
            (content, headers, message) = yield self.rpc_send('configure',content_outgoing)            
                
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def initialize(self, timeout=None):
        """
        """
        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('initialize',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'timeout':None}
            (content, headers, message) = yield self.rpc_send('initialize',content_outgoing)            
        
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def connect(self, timeout=None):
        """
        """
        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('connect',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'timeout':None}
            (content, headers, message) = yield self.rpc_send('connect',content_outgoing)            
        
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def disconnect(self, timeout=None):
        """
        """
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 5
            content_outgoing = {'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('disconnect',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'timeout':None}
            (content, headers, message) = yield self.rpc_send('disconnect',content_outgoing)            
        
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


        
#ResourceAgent
class InstrumentAgent(Process):
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
    version = '0.1.0'
    
    @classmethod
    def get_version(cls):
        """
        Return the software version of the instrument agent.
        """
        return cls.version
    
    
    @defer.inlineCallbacks
    def plc_init(self):
        # Initialize base class.
        Process.plc_init(self)
                        
        """
        The ID of the instrument this agent represents.
        """
        self.instrument_id = self.spawn_args.get('instrument-id',None)
        
        """
        Driver process and client descriptions. Parameter dictionaries
        used to launch driver processes, and dynamically construct driver
        client objects. 
        """
        self.driver_desc = self.spawn_args.get('driver-desc',None)
        self.client_desc = self.spawn_args.get('client-desc',None)
        
        """
        The ProcessDesc object for the driver process.
        """
        self.driver_process_description = None
        if self.driver_desc:
            self.driver_process_description = ProcessDesc(**(self.driver_desc))


        """
        The driver process ID. Attempt to launch the process if the process
        description is set.
        """
        self.driver_pid = None
        if self.driver_process_description:
            self.driver_pid = yield self.spawn_child(self.driver_process_description)
        else:
            yield


        """
        The pubsub client.
        """
        #self.pubsub_client = PubSubClient(proc=self)
        self.pubsub_client = None


        """
        The driver client to communicate with the child driver. Attempt to construct
        this object if there is a driver PID and a client description dict containing
        module and class attributes.
        """
        self.driver_client = None
        if self.driver_pid and self.client_desc and self.client_desc.has_key('module') and self.client_desc.has_key('class'):
            import_str = 'from ' + self.client_desc['module'] + ' import ' + self.client_desc['class']
            ctor_str = 'self.driver_client = ' + self.client_desc['class'] + '(proc=self,target=self.driver_pid)'
            exec import_str
            exec ctor_str

        
        """
	The PubSub origin for the event publisher that this instrument agent uses to
        distribute messages related to generic events that it handles. One queue
        sends all messages, each tagged with an event ID number and optionally
        a channel name if applicable (delimited by "."). If there is no channel name,
        the event applies to the agent. If the channel is a "*", the event applies
        to the instrument as a whole or all channels on the instrument
        For example: 3003.chan1.machine_example_org_14491.357
        @see    ion/services/dm/distribution/events.py
        @see    https://confluence.oceanobservatories.org/display/syseng/CIAD+DM+SV+Notifications+and+Events
        """
        self.event_publisher_origin = str(self.id)
        
        """
        The PubSub publisher for informational/log events 
        """
        self._log_publisher = InfoLoggingEventPublisher(process=self,
                                                           origin=self.event_publisher_origin)
        
        """
        The PubSub publisher for data events
        """
        self._data_publisher = DataBlockEventPublisher(process=self,
                                                           origin=self.event_publisher_origin)
        
        """
        The PubSub publisher for state change events
        """
        self._state_publisher = BusinessStateModificationEventPublisher(process=self,
                                                           origin=self.event_publisher_origin)
    
        """
        A UUID specifying the current transaction. None
        indicates no current transaction.
        """
        self.transaction_id = None
        
        """
        If a transaction expires during an op_ call, this flag is set so
        the transaction can be retired when finishing the call. It is handled
        there to keep the current operation protected until it completes.
        """
        self._transaction_timed_out = False
        
        """
        A twisted delayed function call that implements the transaction timeout.
        This object allows us to cancel the timeout when the transaction is
        ended before timeout.
        """
        self._transaction_timeout_call = None
        
        """
        An integer in seconds for how long to wait to acquire a new
        transaction if a value is not explicitly given.
        """
        self.default_transaction_timeout = 20   
        
        """
        An integer in seconds for the maximum allowable timeout to wait for
        a new transaction.
        """
        self.max_transaction_timeout = 60
    
        """
        An integer in seconds for the minimum required transaction expire
        timeout.
        """
        self.min_transaction_expire_timeout = 1
        
        """
        An integer in seconds for the maximum time a transaction may be open.
        """
        self.transaction_expire_timeout = 300
        
        """
        Upon transaction expire timeout, this flag indicates if the transaction
        can be immediately retired or should be flagged for retire upon
        completion of a protected operation.
        """
        self._in_protected_function = False
    
        """
        A finite state machine to track and manage agent state according to
        the general instrument state model.
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

    """
    do we need this here?
    @defer.inlineCallbacks
    def plc_terminate(self):
        #yield self.driver_pd.shutdown()
        #'In plc terminate'
        #ResourceAgent.plc_terminate()
        pass
    """   
       
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
                
                def transaction_expired():
                    """
                    A callback to expire a transaction. Either retire the transaction
                    directly (no protected call running), or set a flag for a
                    protected call to do the cleanup when finishing.
                    """
                    self._transaction_timeout_call = None                    
                    if self._in_protected_function:
                        self._transaction_timed_out = True
                    else:
                        self._end_transaction(self.transaction_id)

                self._transaction_timeout_call = reactor.callLater(self.transaction_expire_timeout,
                                                                   transaction_expired)
                        
                result['success'] = ['OK']
                result['transaction_id']=self.transaction_id
            else:
                result['success'] = errors['LOCKED_RESOURCE']
                
        else:
            #TODO replay this with callback/timeout logic as necessary
            if self.transaction_id == None:
                self.transaction_id = str(uuid4())

                def transaction_expired():
                    """
                    A callback to expire a transaction. Either retire the transaction
                    directly (no protected call running), or set a flag for a
                    protected call to do the cleanup when finishing.
                    """
                    self._transaction_timeout_call = None                    
                    if self._in_protected_function:
                        self._transaction_timed_out = True
                    else:
                        self._end_transaction(self.transaction_id)

                self._transaction_timeout_call = reactor.callLater(self.transaction_expire_timeout,
                                                                   transaction_expired)                
                
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
            
	# Publish an end transaction message...mainly as a test for now
        yield self._log_publisher.create_and_publish_event(name="Transaction ended!")
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
            self._transaction_timed_out = False
            if self._transaction_timeout_call != None:
                self._transaction_timeout_call.cancel()
                self._transaction_timeout_call = None
                
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

        self._in_protected_function = True
        
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
        
        try:
                     
            if  cmd[0] == 'CI_CMD_STATE_TRANSITION':
                #output = self.agent_fsm.state_transition(cmd[1])
                if cmd[1] not in ci_transition_list:
                    reply['success'] = errors['UNKNOWN_TRANSITION']
                else:
                    # output = self.agent_fsm.state_transition(cmd[1])
                    # TODO FSM and driver integration
                    # The following are stubs for driver integration prior to
                    # state machine integration.
                    if cmd[1] == 'CI_TRANS_INITIALIZE':
                        reply['success'] = ['OK']
                    
                    elif cmd[1] == 'CI_TRANS_GO_ACTIVE':
                        driver_config = self.spawn_args.get('driver-config',None)
                        if driver_config != None:
                            config_reply = yield self.driver_client.configure(driver_config)
                            config_success = config_reply['success']
                            
                            # Could not configure driver.
                            if config_success[0] != 'OK':
                                reply['success'] = config_success
                            
                            # Driver correctly configured.
                            else:
                                
                                # Attempt connect.
                                try:
                                    connect_reply = yield self.driver_client.connect()
                                    
                                # Could not connect, exception raised.
                                except:
                                    reply['success'] = errors['INSTRUMENT_UNREACHABLE']
                                    
                                # Driver responded to connect request.
                                else:
                                    
                                    # Check driver success.
                                    connect_success = connect_reply['success']
                                    if connect_success[0] != 'OK':
                                        reply['success'] = connect_success
                                        
                                    # Driver connection successful.
                                    else:
                                        reply['success'] = ['OK']
                                    
                        else:                        
                            reply['success'] = errors['DRIVER_NOT_CONFIGURED']
    
                    elif cmd[1] == 'CI_TRANS_GO_INACTIVE':
                        disconnect_reply = yield self.driver_client.disconnect()
                        disconnect_success = disconnect_reply['success']
                        if disconnect_success[0] != 'OK':
                            reply['success'] = errors['DISCONNECT_FAILED']
                            
                        else:
                            reply['success'] = ['OK']
                        
                        reply['success'] = ['OK']
                    
                    elif cmd[1] == 'CI_TRANS_CLEAR':
                        reply['success'] = ['OK']
                    
                    elif cmd[1] == 'CI_TRANS_RUN':
                        reply['success'] = ['OK']
    
                    else:
                        reply['success'] = errors['INCORRECT_STATE']
                                       
                        
            elif cmd[0] == 'CI_CMD_TRANSMIT_DATA':
                reply['success'] = errors['NOT_IMPLEMENTED']
            elif cmd[0] == 'CI_CMD_SLEEP':
                if len(cmd) < 2:
                    reply['success'] = errors['REQUIRED_PARAMETER']
                else:
                    time = cmd[1]
                    if not isinstance(time,int):
                        reply['success'] = errors['INVALID_PARAM_VALUE']
                    elif time <=0:
                        reply['success'] = errors['INVALID_PARAM_VALUE']
                    else:
                        yield pu.asleep(time)
                        reply['success'] = ['OK']
                
            else:
                reply['success'] = errors['UNKNOWN_COMMAND']

        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False

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

        self._in_protected_function = True

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
                        
        try:                        
                            
            result = {}                    
            get_errors = False
                    
            # Add each observatory parameter given in params list.
            for arg in params:
                if arg not in ci_param_list and arg != 'all':
                    result[arg] = (errors['INVALID_PARAMETER'], None)
                    get_errors = True                
                    continue
                if arg == 'CI_PARAM_EVENT_PUBLISHER_ORIGIN' or arg=='all':                            
                    if self.event_publisher_origin == None:
                        result['CI_PARAM_EVENT_PUBLISHER_ORIGIN'] = (['OK'],None)
                    else:
                        result['CI_PARAM_EVENT_PUBLISHER_ORIGIN'] = self.event_publisher_origin
                
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
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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

        self._in_protected_function = True
        
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

        
        try:
            
            result = {}
            set_errors = False
            
            # Add each observatory parameter given in params list.
            # Note: it seems like all the current params should be read only by
            # general agent users. 
            for arg in params.keys():
                if arg not in ci_param_list:
                    result[arg] = errors['INVALID_PARAMETER']
                    set_errors = True
                    continue
                
                val = params[arg]
                
                if arg == 'CI_PARAM_DRIVER_ADDRESS':
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
            
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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
        
        self._in_protected_function = True
        
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

        try:
                    
            get_errors = False
            result = {}
                                    
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
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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
        
        self._in_protected_function = True        
        
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

                    
        try:
            
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
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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

        self._in_protected_function = True
        
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
        
        try:
            
            get_errors = False
            result = {}
            
            # Do the work here.
            # Set up the result message.
            for arg in params:
                if arg not in capabilities_list and arg != 'all':
                    result[arg] = (errors['INVALID_CAPABILITY'],None)
                    get_errors = True
                    continue
                
                if arg == 'CAP_OBSERVATORY_COMMANDS' or arg == 'all':
                    result['CAP_OBSERVATORY_COMMANDS'] = (['OK'],ci_command_list)
                    
                if arg == 'CAP_OBSERVATORY_PARAMS' or arg == 'all':
                    result['CAP_OBSERVATORY_PARAMS'] = (['OK'],ci_param_list)
                    
                if arg == 'CAP_OBSERVATORY_STATUSES' or arg == 'all':
                    result['CAP_OBSERVATORY_STATUSES'] = (['OK'],ci_status_list)
                    
                if arg == 'CAP_METADATA' or arg == 'all':
                    result['CAP_METADATA'] = (['OK'],metadata_list)
                    
                if arg == 'CAP_DEVICE_COMMANDS' or arg == 'all':
                    #TDOD driver integration.
                    #dvr_content = {'params':'CAP_DEVICE_COMMANDS'}
                    #dvr_result = yield self.driver_client.rpc_send('get_capabilities',dvr_content)
                    #dvr_success = dvr_result[0]
                    #dvr_val = dvr_result[1]['CAP_DEVICE_COMMANDS']
                    dvr_val = (['OK'],['device_command_1','device_command_2'])
                    result['CAP_DEVICE_COMMANDS'] = dvr_val
                    if dvr_val[0][0] != 'OK':
                        get_errors = True
                    
                if arg == 'CAP_DEVICE_PARAMS' or arg == 'all':
                    #TDOD driver integration.
                    #dvr_content = {'params':'CAP_DEVICE_PARAMS'}
                    #dvr_result = yield self.driver_client.rpc_send('get_capabilities',dvr_content)
                    #dvr_success = dvr_result[0]
                    #dvr_val = dvr_result[1]['CAP_DEVICE_PARAMS']
                    dvr_val = (['OK'],['device_param_1','device_param_2','device_param_3'])
                    result['CAP_DEVICE_PARAMS'] = dvr_val
                    if dvr_val[0][0] != 'OK':
                        get_errors = True
                    
                if arg == 'CAP_DEVICE_STATUSES' or arg == 'all':
                    #TODO driver integration.
                    #dvr_content = {'params':'CAP_DEVICE_STATUSES'}
                    #dvr_result = yield self.driver_client.rpc_send('get_capabilities',dvr_content)
                    #dvr_success = dvr_result[0]
                    #dvr_val = dvr_result[1]['CAP_DEVICE_STATUSES']
                    dvr_val = (['OK'],['device_status_1','device_status_2','device_status_3'])
                    result['CAP_DEVICE_STATUSES'] = dvr_val
                    if dvr_val[0][0] != 'OK':
                        get_errors = True
     
            
            if get_errors:
                success = errors['GET_OBSERVATORY_ERR']
            else:
                success = ['OK']
                
            reply['success'] = success
            reply['result'] = result
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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

        self._in_protected_function = True

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
                    
        try:                    
                    
            dvr_result = yield self.driver_client.execute(channels,command)
            
            reply['success'] = dvr_result['success']
            reply['result'] = dvr_result['result']
        
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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
        
        self._in_protected_function = True
        
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
                    
        try:
            
            dvr_result = yield self.driver_client.get(params)
            
            reply['success'] = dvr_result['success']
            reply['result'] = dvr_result['result']
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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
        
        self._in_protected_function = True
        
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
                    
        try:
            
            dvr_result = yield self.driver_client.set(params)
            
            reply['success'] = dvr_result['success']
            reply['result'] = dvr_result['result']
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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
        
        self._in_protected_function = True
        
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
                    
                    
        try:
            
            dvr_content = {'params':params}
            dvr_result = yield self.driver_client.rpc_send('get_metadata',dvr_content)
            
            reply['success'] = dvr_result['success']
            reply['result'] = dvr_result['result']
        
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
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
        
        self._in_protected_function = True
        
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
                    
        try:
            
            dvr_content = {'params':params}
            dvr_result = yield self.driver_client.rpc_send('get_status',dvr_content)
        
            reply['success'] = dvr_result['success']
            reply['result'] = dvr_result['result']
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
        yield self.reply_ok(msg,reply)


    # TODO: decide how this is used with direct access mode. Should
    # transactions be enabled here, e.g. only one user in direct access mode,
    # et cetera.
    @defer.inlineCallbacks
    def op_execute_device_direct(self,content,headers,msg):
        """
        Execute untranslated byte data commands on the device.
        Must be in direct access mode and possess the correct transaction_id key
        for the direct access session.
        @param content A dict {'bytes':block_of_data,'transaction_id':transaction_id}
        @retval A dict {'success':success,'result':block_of_data}.
        """
        
        self._in_protected_function = True
        
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
                    
        try:                    
                    
            dvr_content = {'bytes':bytes}
            dvr_result = yield self.driver_client.rpc_send('execute_direct',dvr_content)
        
            reply['success'] = dvr_result['success']
            reply['result'] = dvr_result['result']
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
        yield self.reply_ok(msg,reply)
            

    ############################################################################
    #   Publishing Methods
    ############################################################################


    @defer.inlineCallbacks
    def op_driver_event_occurred(self, content, headers, msg):
        """
        Called by the driver to announce the occurance of an event. The agent
        take appropriate action including state transitions, data formatting
        and publication. This method must be called by a child process of the
        agent.
        @param content a dict with 'type' and 'transducer' strings and 'value'
            object.
        """
        
        assert isinstance(content,dict), 'Expected a content dict.'
        
        type = content.get('type',None)
        transducer = content.get('transducer',None)
        value = content.get('value',None)
        
        assert isinstance(type,str), 'Expected a type string.'
        assert isinstance(transducer,str), 'Expected a transducer string.'
        assert value != None, 'Expected a value.'
        
        if not (self._is_child_process(headers['sender-name'])):
            yield self.reply_err(msg,
                                 'driver event occured evoked from a non-child process')
            return
        
        
        self._debug_print_driver_event(type,transducer,value)
        
                
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
                yield self._data_publisher.create_and_publish_event( \
                    origin="%s.%s" % (content["Transducer"], self.event_publisher_origin),
                    description=content["Value"])
            elif ((content["Type"] == publish_msg_type["Error"])
                or (content["Value"] == "ConfigChange")):
                yield self._log_publisher.create_and_publish_event( \
                    origin="%s.%s" % (content["Transducer"], self.event_publisher_origin),
                    description=content["Value"])
            elif (content["Type"] == publish_msg_type["StateChange"]):
                yield self._state_publisher.create_and_publish_event( \
                    origin="%s.%s" % (content["Transducer"], self.event_publisher_origin),
                    description=content["Value"])
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
                yield self._log_publisher.create_and_publish_event( \
                    origin=self.event_publisher_origin, description=value)
            
        if (type == publish_msg_type["StateChange"]):
                yield self._state_publisher.create_and_publish_event( \
                    origin=self.event_publisher_origin, description=value)
        
    ############################################################################
    #   Nonpublic methods.
    ############################################################################

    @defer.inlineCallbacks
    def op_sleep(self, content, headers, msg):
        """
        Sleep the agent in a blocking call for a specified time.
        @param time an int for number of seconds to sleep.
        """
        
        self._in_protected_function = True
        
        assert(isinstance(content,dict)), 'Expected a dict content.'
        assert(content.has_key('time')), 'Expected time.'
        assert(content.has_key('transaction_id')), 'Expected a transaction_id.'
        
        time = content['time']
        tid = content['transaction_id']

        assert(isinstance(tid,str)), 'Expected a transaction_id str.'
        assert(isinstance(time,int)), 'Expected an int time.'
        assert(time>0), 'Expected a positive time.'
        
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
                    
        try:
            
            yield pu.asleep(time)
        
        # Transaction clean up. End implicit or expired transactions.        
        finally:
            if (tid == 'create') or (self._transaction_timed_out == True):
                self._end_transaction(self.transaction_id)
            self._in_protected_function = False
                    
        yield self.reply_ok(msg,reply)
        
        
        
        
    ############################################################################
    #   Other.
    ############################################################################
        
    def _get_buffer_size(self):
        """
        Return the total size in characters of the data buffer.
        Assumes the buffer is a list of string data lines.
        """
        return sum(map(lambda x: len(x),self.data_buffer))
        
        
    def _debug_print_driver_event(self,type,transducer,value):
        """
        """
        if DEBUG_PRINT:
            if isinstance(value,str):
                print 'driver event: '+ type + ',  '+ transducer + ',  ' + value
                
            elif isinstance(value,dict):
                print 'driver event: '+ type + ',  '+ transducer
                for (key,val) in value.iteritems():
                    print str(key), ' ', str(val)
            else:
                print 'driver event: '+ type + ',  '+ transducer
                print value
        
    def _debug_print(self,event=None,value=None):
        """
        """
        pass
    

    
#ResourceAgentClient
class InstrumentAgentClient(ProcessClient):
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
        assert(isinstance(content,dict)), 'Expected dict result'
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
        @retval Reply dict {'success':success,'result':command-specific,
        'transaction_id':transaction_id}.
        """
        
        assert(isinstance(command,list)), 'Expected a command list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'

        content = {'command':command,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('execute_observatory',
                                                         content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)
        

    @defer.inlineCallbacks
    def get_observatory(self,params,transaction_id='none'):
        """
        Get data from the cyberinfrastructure side of the agent (registry info,
        topic locations, messaging parameters, process parameters, etc.)
        @param params A paramter list [param_arg, ,param_arg].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'
        @retval A reply dict {'success':success,'result':{param_arg:(success,val),
            ...,param_arg:(success,val)},'transaction_id':transaction_id)        
        """
        
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_observatory',
                                                         content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)
        
        

    @defer.inlineCallbacks
    def set_observatory(self,params,transaction_id='none'):
        """
        Set parameters related to the infrastructure side of the agent
        (registration information, location, network addresses, etc.)
        @param params A parameter-value dict {'params':{param_arg:val,
            ..., param_arg:val}.
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'        
        @retval Reply dict
            {'success':success,'result':{param_arg:success,...,param_arg:success},
                'transaction_id':transaction_id}.        
        """
        assert(isinstance(params,dict)), 'Expected a parameter-value dict.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('set_observatory',
                                                         content)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_observatory_metadata(self,params,transaction_id='none'):
        """
        Retrieve metadata about the observatory configuration parameters.
        @param params A metadata parameter list [(param_arg,meta_arg),...,
            (param_arg,meta_arg)].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                
        @retval A reply dict {'success':success,
            'result':{param_arg:{meta_arg):(success,val),...,meta_arg:(success,val)},
            ...param_arg:{meta_arg:(success,val),...,meta_arg:(success,val)}},
            'transaction_id':transaction_id}.
        """
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_observatory_metadata',
                                                         content)
        
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
            {'success':success,'result':{chan_arg:(success,command_specific_values),
            ...,chan_arg:(success,command_specific_values)},
            'transaction_id':transaction_id}. 
        """
        assert(isinstance(channels,list)), 'Expected a channels list.'
        assert(isinstance(command,list)), 'Expected a command list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        rpc_timeout = 30
        
        content = {'channels':channels,'command':command,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('execute_device',
                                                         content,timeout=rpc_timeout)
        
        assert(isinstance(content,dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_device(self,params,transaction_id='none'):
        """
        Get configuration parameters from the instrument. 
        @param params A parameters list [(chan_arg,param_arg),...,(chan_arg,param_arg)].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                                        
        @retval A reply dict
            {'success':success,'result':{(chan_arg,param_arg):(success,val),
            ...,(chan_arg,param_arg):(success,val)},
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
        @param params A parameter-value dict {(chan_arg,param_arg):val,
        ...,(chan_arg,param_arg):val}.
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                                
        @retval Reply dict
            {'success':success,'result':{(chan_arg,param_arg):success,
            ...,chan_arg,param_arg):success},
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
        @param params A metadata parameter list [(chan_arg,param_arg,meta_arg),
        ...,(chan_arg,param_arg,meta_arg)].
        @param transaction_id A transaction ID uuid4 or string 'create,' 'none.'                                        
        @retval Reply dict
            {'success':success,'result':{(chan_arg,param_arg,meta_arg):(success,val),...,
            chan_arg,param_arg,meta_arg):(success,val)}, 'transaction_id':transaction_id}.
        """
        assert(isinstance(params,list)), 'Expected a parameter list.'
        assert(isinstance(transaction_id,str)), 'Expected a transaction_id str.'
        
        content = {'params':params,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('get_device_metadata',
                                                         content)
        
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
        (content,headers,messaage) = yield self.rpc_send('get_device_status',
                                                         content)
        
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
        (content,headers,messaage) = yield self.rpc_send('execute_device_direct',
                                                         content)
        
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
    
    ############################################################################
    #   Nonpublic interface.
    ############################################################################

    @defer.inlineCallbacks
    def sleep(self,time,transaction_id='none'):
        """
        Sleep the agent in a blocking call for a specified time.
        @param time an int for number of seconds to sleep.
        """
        
        assert(isinstance(time,int)), 'Expected an int time.'
        assert(time>0), 'Expected a positive time.'

        content = {'time':time,'transaction_id':transaction_id}
        (content,headers,messaage) = yield self.rpc_send('sleep',content)
        
    
    
# Spawn of the process using the module name
factory = ProcessFactory(InstrumentAgent)


