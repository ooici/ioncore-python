#!/usr/bin/env python

"""
@file ion/play/hello_resource.py
@author David Stuebe
@brief An example service definition that can be used as template for resource management.
"""

import ion.util.ionlog
import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils
from ion.core import ioninit
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.services.coi.resource_registry_beta.resource_client import ResourceClientError, ResourceInstanceError
from twisted.internet import defer


CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)

resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)
resource_response_type = object_utils.create_type_identifier(object_id=12, version=1)

exchangespace_type = object_utils.create_type_identifier(object_id=1001, version=1)
exchangename_type = object_utils.create_type_identifier(object_id=1001, version=1)
queue_type = object_utils.create_type_identifier(object_id=1001, version=1)
binding_type = object_utils.create_type_identifier(object_id=1001, version=1)


class ExchangeManagementError(Exception):
    """
    An exception class for the Exchange Management system.
    """


class ServiceHelper:
    def __init__(self, proc):
        self.proc = proc
        self.rc = ResourceClient(proc=proc)
        self.mc = MessageClient(proc=proc)

    
    def check_request(self, msg, type):
        """
        """
        if msg.MessageType != resource_request_type:
            raise ExchangeManagementError('wrong message type: %s' % str(msg.MessageType))
        if msg.HasField('resource_reference'):
            raise ExchangeManagementError('resource_reference field expected to be unset, received: %s' % msg.resource_reference)
        
    
    @defer.inlineCallbacks    
    def create_object(self, msg, name, description):
        object = msg.configuration
        type = object.ObjectType
        object = yield self.rc.create_instance(type, name, description)
        yield defer.returnValue(object)
        
        
    @defer.inlineCallbacks    
    def push_object(self, object):
        yield self.rc.put_instance(object)
        
        response = yield self.mc.create_instance(exchangespace_type, name='create_instrument_resource response')
        response.resource_reference = self.rc.reference_instance(object)
        response.configuration = object.ResourceObject
        response.result = 'Created'
        yield defer.returnValue(response)

        
        
class ClientHelper:

    def __init__(self, proc):
        self.proc = proc
        self.mc = MessageClient(proc=proc)
    
    @defer.inlineCallbacks    
    def create_object(self, type):
        msg = yield self.mc.create_instance(resource_request_type, name='create_object')
        msg.configuration = msg.CreateObject(type)        
        defer.returnValue(msg)