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

from ion.core.messaging.message_client import MessageClient

from ion.services.coi.resource_registry.resource_client import ResourceClient

from ion.core.object import object_utils

# from net.ooici.play policy_protected.proto
PROTECTED_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=20037, version=1)
PROTECTED_RESOURCE_FIND_REQ_TYPE = object_utils.create_type_identifier(object_id=20038, version=1)
PROTECTED_RESOURCE_FIND_RSP_TYPE = object_utils.create_type_identifier(object_id=20039, version=1)
PROTECTED_RESOURCE_CREATE_REQ_TYPE = object_utils.create_type_identifier(object_id=20040, version=1)
PROTECTED_RESOURCE_CREATE_RSP_TYPE = object_utils.create_type_identifier(object_id=20041, version=1)
PROTECTED_RESOURCE_UPDATE_REQ_TYPE = object_utils.create_type_identifier(object_id=20042, version=1)
PROTECTED_RESOURCE_UPDATE_RSP_TYPE = object_utils.create_type_identifier(object_id=20043, version=1)
PROTECTED_RESOURCE_DELETE_REQ_TYPE = object_utils.create_type_identifier(object_id=20044, version=1)
PROTECTED_RESOURCE_DELETE_RSP_TYPE = object_utils.create_type_identifier(object_id=20045, version=1)

RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
RESOURCE_CFG_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)

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
        self.mc = MessageClient(proc = self)
        self.rc = ResourceClient(proc=self)

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


    @defer.inlineCallbacks
    def op_hello_create_resource(self, content, headers, msg):
        name = content.configuration.name
        description = content.configuration.description
                
        # Use the resource client to create a resource!
        resource = yield self.rc.create_instance(PROTECTED_RESOURCE_TYPE, ResourceName=name, ResourceDescription=description)

        yield self.rc.put_instance(resource)
        
        response = yield self.mc.create_instance(RESOURCE_CFG_RESPONSE_TYPE, MessageName='hello_create_resource response')
        response.configuration = response.CreateObject(PROTECTED_RESOURCE_CREATE_RSP_TYPE)

        response.configuration.resource_id = resource.ResourceIdentity
                
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_hello_find_resource(self, content, headers, msg):
        resource_id = content.configuration.resource_id

        resource = yield self.rc.get_instance(resource_id)
                
        response = yield self.mc.create_instance(RESOURCE_CFG_RESPONSE_TYPE, MessageName='hello_find_resource response')
        response.configuration = response.CreateObject(PROTECTED_RESOURCE_FIND_RSP_TYPE)

        response.configuration.resources.add()
        response.configuration.resources[0].resource_id = resource_id
        response.configuration.resources[0].name = resource.name
        response.configuration.resources[0].description = resource.description
                
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_hello_update_resource(self, content, headers, msg):
        resource_id = content.configuration.resource_id
        name = content.configuration.name
        description = content.configuration.description

        resource = yield self.rc.get_instance(resource_id)
        resource.name = name
        resource.description = description

        yield self.rc.put_instance(resource)
        
        response = yield self.mc.create_instance(RESOURCE_CFG_RESPONSE_TYPE, MessageName='hello_update_resource response')
        response.configuration = response.CreateObject(PROTECTED_RESOURCE_UPDATE_RSP_TYPE)

        response.configuration.status = 'OK'
                
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_hello_delete_resource(self, content, headers, msg):
        for resource_id in content.configuration.resource_ids:
            print resource_id
            resource = yield self.rc.get_instance(resource_id)
            print resource
            # TODO actually delete object
        
        response = yield self.mc.create_instance(RESOURCE_CFG_RESPONSE_TYPE, MessageName='hello_delete_resource response')
        response.configuration = response.CreateObject(PROTECTED_RESOURCE_DELETE_RSP_TYPE)

        response.configuration.status = 'OK'
                
        yield self.reply_ok(msg, response)
    
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

    @defer.inlineCallbacks
    def hello_create_resource(self, create_msg, user='ANONYMOUS'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send_protected('hello_create_resource', create_msg, user, '0')
        #print 'Service reply: ' +str(content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def hello_find_resource(self, find_msg, user='ANONYMOUS'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send_protected('hello_find_resource', find_msg, user, '0')
        #print 'Service reply: ' +str(content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def hello_update_resource(self, update_msg, user='ANONYMOUS'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send_protected('hello_update_resource', update_msg, user, '0')
        #print 'Service reply: ' +str(content)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def hello_delete_resource(self, delete_msg, user='ANONYMOUS'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send_protected('hello_delete_resource', delete_msg, user, '0')
        #print 'Service reply: ' +str(content)
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(HelloPolicy)