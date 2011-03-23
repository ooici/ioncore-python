#!/usr/bin/env python

"""
@file ion/play/hello_policy.py
@author Thomas Lennan
@brief An example service illustrating policy enforcement functionality.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

class HelloPolicy(ServiceProcess):
    """
    First level service interface
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_policy',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        log.info('HelloPolicy.__init__()')

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_hello_anonymous_request(self, content, headers, msg):
        log.info('op_hello_anonymous_request: '+str(headers))
        
        # This service call is labeled as requiring anonymous level authority
        # and should work for all users. Return content of headers user id
        # field to confirm.
        if headers:
            if 'user-id' in headers:
                yield self.reply_ok(msg, {'value':headers['user-id']}, {})
                return
                
        estr = 'Test failed. No headers or user-id in headers.'
        log.exception(estr)
        yield self.reply_err(msg, estr)
        return

    @defer.inlineCallbacks
    def op_hello_authenticated_request(self, content, headers, msg):
        log.info('op_hello_anonymous_request: '+str(headers))
        
        # This service call is labeled as requiring authenticated level authority
        # and should not work for anonymous users. Return content of headers user id
        # field to confirm.
        if headers:
            if 'user-id' in headers:
                if headers['user-id'] == 'ANONYMOUS':
                    estr = 'Test failed. Anonymous user id.'
                    log.exception(estr)
                    yield self.reply_err(msg, estr)
                    return
                else:
                    yield self.reply_ok(msg, {'value':headers['user-id']}, {})
                    return
                
        estr = 'Test failed. No headers or user-id in headers.'
        log.exception(estr)
        yield self.reply_err(msg, estr)
        return

class HelloPolicyClient(ServiceClient):
    """
    Basic service client to make RPC calls to service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_policy"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def hello_request(self, request_op, user=None, expiry=None):
        yield self._check_init()
        if request_op == 'hello_anonymous_request':
            if user == None:
                (content, headers, msg) = yield self.rpc_send(request_op, {'data': 'some data'})
            else:
                (content, headers, msg) = yield self.rpc_send_protected(request_op, {'data': 'some data'}, user, expiry)
        elif request_op == 'hello_authenticated_request':
            if user == None:
                (content, headers, msg) = yield self.rpc_send(request_op, {'data': 'some data'})
            else:
                (content, headers, msg) = yield self.rpc_send_protected(request_op, {'data': 'some data'}, user, expiry)
        else:
            content = 'Unexpected request type: %s' % request_op
        log.info('Service reply: '+str(content))
        #print 'Service reply: ' +str(content)
        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProcessFactory(HelloPolicy)