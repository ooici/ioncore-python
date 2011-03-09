#!/usr/bin/env python

"""
@file ion/play/hello_identity_delegate.py
@author Thomas Lennan
@brief An example service illustrating identity passing functionality
on delegation to another service.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.play.hello_identity import HelloIdentityClient

class HelloIdentityDelegate(ServiceProcess):
    """
    'Application' service interface which delegates to other services
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_identity_delegate',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        self.hc = HelloIdentityClient(proc=self)
        log.info('HelloIdentityDelegate.__init__()')

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_hello_anonymous_request_delegate(self, content, headers, msg):
        log.info('op_hello_anonymous_request_delegate: '+str(headers))
        
        # Validate that the headers contain a user id and that the value is 'ANONYMOUS'
        if headers:
            if 'user-id' in headers:
                if headers['user-id'] == 'ANONYMOUS':
                    # Nothing to do from an identity point of view.
                    # 'ANONYMOUS' will automatically be added to outgoing request message
                    result = yield self.hc.hello_request_from_delegate('hello_anonymous_request')
                    yield self.reply_ok(msg, result, {})
                    return
                
        estr = 'Message not from ANONYMOUS'
        log.exception(estr)
        yield self.reply_err(msg, estr)
        return

    @defer.inlineCallbacks
    def op_hello_user_request_delegate(self, content, headers, msg):
        log.info('op_hello_user_request_delegate: '+str(headers))
        
        # Validate that the headers contain a user id and that the value is not 'ANONYMOUS'
        if headers:
            if 'user-id' in headers:
                if headers['user-id'] != 'ANONYMOUS':
                    # Nothing to do from an identity point of view.
                    # user id will automatically be added to outgoing request message
                    result = yield self.hc.hello_request_from_delegate('hello_user_request')
                    yield self.reply_ok(msg, result, {})
                    return
                
        estr = 'Message not from user'
        log.exception(estr)
        yield self.reply_err(msg, estr)
        return

    @defer.inlineCallbacks
    def op_hello_user_request_delegate_switch_user(self, content, headers, msg):
        log.info('op_hello_user_request_delegate: '+str(headers))
        
        # Validate that the headers contain a user id and that the value is not 'ANONYMOUS'
        if headers:
            if 'user-id' in headers:
                if headers['user-id'] != 'ANONYMOUS':
                    # Nothing to do from an identity point of view.
                    # user id will automatically be added to outgoing request message
                    result = yield self.hc.hello_request('hello_user_request','NEWUSER','11111111')
                    yield self.reply_ok(msg, result, {})
                    return
                
        estr = 'Message not from user'
        log.exception(estr)
        yield self.reply_err(msg, estr)
        return

class HelloIdentityDelegateClient(ServiceClient):
    """
    Basic service client to make RPC calls to first level service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_identity_delegate"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def hello_request(self, request_op, user=None, expiry=None):
        yield self._check_init()
        if request_op == 'hello_anonymous_request_delegate':
            (content, headers, msg) = yield self.rpc_send(request_op, {'data': 'some data'})
        elif request_op == 'hello_user_request_delegate':
            (content, headers, msg) = yield self.rpc_send_protected(request_op, {'data': 'some data'}, user, expiry)
        elif request_op == 'hello_user_request_delegate_switch_user':
            (content, headers, msg) = yield self.rpc_send_protected(request_op, {'data': 'some data'}, user, expiry)
        else:
            content = 'Unexpected request type: %s' % request_op
        log.info('Service reply: '+str(content))
        #print 'Service reply delegate: ' +str(content)
        defer.returnValue(str(content))
        
# Spawn of the process using the module name
factory = ProcessFactory(HelloIdentityDelegate)