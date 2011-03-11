#!/usr/bin/env python

"""
@file ion/play/hello_identity.py
@author Thomas Lennan
@brief An example service illustrating identity passing functionality.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

class HelloIdentity(ServiceProcess):
    """
    First level service interface
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_identity',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        log.info('HelloIdentity.__init__()')

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_hello_anonymous_request(self, content, headers, msg):
        log.info('op_hello_anonymous_request: '+str(headers))
        
        # Validate that the headers contain a user id and that the value is 'ANONYMOUS'
        if headers:
            if 'user-id' in headers:
                if headers['user-id'] == 'ANONYMOUS':
                    yield self.reply_ok(msg, {'value':'Hello there ANONYMOUS'}, {})
                    return
                
        estr = 'Message not from ANONYMOUS'
        log.exception(estr)
        yield self.reply_err(msg, estr)
        return

    @defer.inlineCallbacks
    def op_hello_user_request(self, content, headers, msg):
        log.info('op_hello_user_request: '+str(headers))
        
        # Validate that the headers contain a user id and that the value is not 'ANONYMOUS'
        if headers:
            if 'user-id' in headers:
                if headers['user-id'] != 'ANONYMOUS':
                    yield self.reply_ok(msg, {'value':'Hello there '+str(headers['user-id'])+' expiry: '+str(headers['expiry'])}, {})
                    return
                
        estr = 'Message not from user'
        log.exception(estr)
        yield self.reply_err(msg, estr)
        return

class HelloIdentityClient(ServiceClient):
    """
    Basic service client to make RPC calls to first level service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_identity"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def hello_request(self, request_op, user=None, expiry=None):
        yield self._check_init()
        if request_op == 'hello_anonymous_request':
            (content, headers, msg) = yield self.rpc_send(request_op, {'data': 'some data'})
        elif request_op == 'hello_user_request':
            # Note the use of the new RPC send protected method, which allows
            # the passing of a user id and expiry
            (content, headers, msg) = yield self.rpc_send_protected(request_op, {'data': 'some data'}, user, expiry)
        else:
            content = 'Unexpected request type in delegate: %s' % request_op
        log.info('Service reply: '+str(content))
        #print 'Service reply: ' +str(content)
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def hello_request_from_delegate(self, request_op):
        yield self._check_init()
        if request_op == 'hello_anonymous_request':
            (content, headers, msg) = yield self.rpc_send(request_op, {'data': 'some data'})
        elif request_op == 'hello_user_request':
            (content, headers, msg) = yield self.rpc_send(request_op, {'data': 'some data'})
        else:
            content = 'Unexpected request type in delegate: %s' % request_op
        log.info('Service reply: '+str(content))
        #print 'Service reply: ' +str(content)
        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProcessFactory(HelloIdentity)