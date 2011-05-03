#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/instrument_driver.py
@author Steve Foley
@author Edward Hunter
@brief Instrument driver and client base classes.
"""


from twisted.internet import defer, reactor

import ion.util.ionlog
from ion.core.process.process import Process, ProcessClient



log = ion.util.ionlog.getLogger(__name__)


class InstrumentDriver(Process):
    """
    Instrument driver base class for instrument specific driver subclasses.
    """
    
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

        
    def op_get(self, content, headers, msg):
        """
        Get configuration parameters from the device. 
        @param content A list [(chan_arg,param_arg),...,(chan_arg,param_arg)].
        @retval A reply message with a dict
            {'success':success,'result':{(chan_arg,param_arg):(success,val),...
                ,(chan_arg,param_arg):(success,val)}}        
        """


    def op_set(self, content, headers, msg):
        """
        Set parameters to the device.
        @param content A dict {(chan_arg,param_arg):val,...,
            (chan_arg,param_arg):val}.
        @retval Reply message with a dict
            {'success':success,'result':
                {(chan_arg,param_arg):success,...,chan_arg,param_arg):success}}.        
        """


    def op_get_metadata(self, content, headers, msg):
        """
        Retrieve metadata for the device, its transducers and parameters.
        @param content A list:[(chan_arg,param_arg,meta_arg),...,
            (chan_arg,param_arg,meta_arg)] specifying the metadata to retrieve.
        @retval Reply message with a dict {'success':success,'result':
                {(chan_arg,param_arg,meta_arg):(success,val),...,
                chan_arg,param_arg,meta_arg):(success,val)}}.        
        """
        
        

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


    def op_initialize(self, content, headers, msg):
        """
        Restore driver to a default, unconfigured state.
        @retval A reply message with a dict {'success':success,'result':None}.
        """


    def op_configure(self, content, headers, msg):
        """
        Configure the driver to establish communication with the device.
        @param content a dict containing required and optional
            configuration parameters.
        @retval A reply message dict {'success':success,'result':content}.
        """
        

    def op_connect(self, content, headers, msg):
        """
        Establish connection to the device.
        @retval A dict {'success':success,'result':None} giving the success
            status of the connect operation.
        """


    def op_disconnect(self, content, headers, msg):
        """
        Close connection to the device.
        @retval A dict {'success':success,'result':None} giving the success
            status of the disconnect operation.
        """

    def op_get_state(self,content,headers,msg):
        """
        Retrive the current state of the driver.
        @retval The current instrument state, from sbe37_state_list
        (see ion.agents.instrumentagents.instrument_agent_constants.
            device_state_list for the common states.)
         """


class InstrumentDriverClient(ProcessClient):
    """
    Driver client base class for instrument specific subclasses.
    Provides RPC messaging to the driver service.    
    """

    @defer.inlineCallbacks
    def execute(self,channels,command,timeout=None):
        """
        Execute a driver command. Commands may be
        common or specific to the device, with specific commands known through
        knowledge of the device or a previous get_capabilities query.
        @param channels a channel list [chan_arg,...,chan_arg]
        @param command a command list[command,arg,...,argN]
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval A reply message with a dict
            {'success':success,
            'result':{chan_arg:(success,command_specific_values),...,
            chan_arg:(success,command_specific_values)}}. 
        """

        assert(isinstance(channels, (list,tuple))), 'Expected list or tuple channels.'
        assert(isinstance(command, (list,tuple))), 'Expected list or tuple command.'
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
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
        Get configuration parameters from the device. 
        @param params a list [(chan_arg,param_arg),...,(chan_arg,param_arg)].
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval A reply message with a dict
            {'success':success,'result':{(chan_arg,param_arg):(success,val),...
                ,(chan_arg,param_arg):(success,val)}}        
        """
                
        assert(isinstance(params, (list, tuple))), 'Expected a params list or tuple.'                
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
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
        Set parameters to the device.
        @param params A dict {(chan_arg,param_arg):val,...,
            (chan_arg,param_arg):val}.
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval Reply message with a dict
            {'success':success,'result':
                {(chan_arg,param_arg):success,...,chan_arg,param_arg):success}}.        
        """
        
        
        assert(isinstance(params, dict)), 'Expected a params dict.'                
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
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
        Obtain the status of the device. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param params A list [(chan_arg,status_arg),...,
            (chan_arg,status_arg)] specifying the status arguments to query.
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval A reply message with a dict
            {'success':success,'result':{(chan_arg,status_arg):(success,val),
                ...,chan_arg,status_arg):(success,val)}}.
        """
        
        assert(isinstance(params, (list, tuple))), 'Expected a params list or tuple.'        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
            content_outgoing = {'params':params,'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('get_status',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'params':params,'timeout':None}
            (content, headers, message) = yield self.rpc_send('get_status',content_outgoing)            
                
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def initialize(self, timeout=None):
        """
        Restore driver to a default, unconfigured state.
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval A reply message with a dict {'success':success,'result':None}.
        """
        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
            content_outgoing = {'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('initialize',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'timeout':None}
            (content, headers, message) = yield self.rpc_send('initialize',content_outgoing)            
        
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def configure(self, params, timeout=None):
        """
        Configure the driver to establish communication with the device.
        @param params a dict containing required and optional
            configuration parameters.
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval A reply message dict {'success':success,'result':content}.
        """
        assert(isinstance(params, dict)), 'Expected a params dict.'        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
            content_outgoing = {'params':params,'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('configure',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'params':params,'timeout':None}
            (content, headers, message) = yield self.rpc_send('configure',content_outgoing)            
                
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def connect(self, timeout=None):
        """
        Establish connection to the device.
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval A dict {'success':success,'result':None} giving the success
            status of the connect operation.
        """
        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
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
        Close connection to the device.
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval A dict {'success':success,'result':None} giving the success
            status of the disconnect operation.
        """
        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
            content_outgoing = {'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('disconnect',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'timeout':None}
            (content, headers, message) = yield self.rpc_send('disconnect',content_outgoing)            
        
        assert(isinstance(content, dict)), 'Expected a reply content dict.'        
        defer.returnValue(content)


    @defer.inlineCallbacks
    def get_state(self, timeout=None):
        """
        Retrive the current state of the driver.
        @param timeout optional timeout to the driver causes the rpc timeout
            to be set slightly longer.
        @retval The current instrument state, from the instrument specific
            state list.
        (see ion.agents.instrumentagents.instrument_agent_constants.
            device_state_list for the common states.)
        """
        
        if timeout != None:
            assert(isinstance(timeout, int)), 'Expected a timeout int.'
            assert(timeout>0), 'Expected a positive timeout.'
            rpc_timeout = timeout + 20
            content_outgoing = {'timeout':timeout}
            (content, headers, message) = yield self.rpc_send('get_state',content_outgoing,timeout=rpc_timeout)            
            
        else:            
            content_outgoing = {'timeout':None}
            (content, headers, message) = yield self.rpc_send('get_state',content_outgoing)            
                
        assert(isinstance(content,str)), 'Expected a state string reply.'
        defer.returnValue(content)


