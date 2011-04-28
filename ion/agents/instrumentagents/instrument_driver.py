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
