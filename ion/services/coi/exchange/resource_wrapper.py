#!/usr/bin/env python

"""
@file ion.services.coi.exchange.exchange_resource.py
@author Brian Fox
@brief Provides convenience methods to manage exchange management resources.
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


# Used for manipulating the resource registry 

resource_request_type      = object_utils.create_type_identifier(object_id=10, version=1)
resource_response_type     = object_utils.create_type_identifier(object_id=12, version=1)


# All the various google buffer protos are listed below.  It's unnecessary 
# to redefine these in other modules.  Instead use soemthing like:  
#
# import ion.services.coi.exchange.exchange_resources as bp 
# my_type = bp.queue_type


broker_type                = object_utils.create_type_identifier(object_id=1001, version=1)
exchangespace_type         = object_utils.create_type_identifier(object_id=1002, version=1)
exchangename_type          = object_utils.create_type_identifier(object_id=1003, version=1)
queue_type                 = object_utils.create_type_identifier(object_id=1004, version=1)
binding_type               = object_utils.create_type_identifier(object_id=1005, version=1)
amqpexchangemapping_type   = object_utils.create_type_identifier(object_id=1006, version=1)
amqpqueuemapping_type      = object_utils.create_type_identifier(object_id=1007, version=1)
amqpbrokercredentials_type = object_utils.create_type_identifier(object_id=1008, version=1)
brokerfederation_type      = object_utils.create_type_identifier(object_id=1009, version=1)
hardwaremapping_type       = object_utils.create_type_identifier(object_id=1010, version=1)


# Defined for easier unit tests that loop through all prototypes.

all_types = {
                "exchangespace_type"         : exchangespace_type,
                "exchangename_type"          : exchangename_type,
                "queue_type"                 : queue_type,
                "binding_type"               : binding_type,
                "amqpexchangemapping_type"   : amqpexchangemapping_type,
                "amqpqueuemapping_type"      : amqpqueuemapping_type,
                "amqpbrokercredentials_type" : amqpbrokercredentials_type,
                "brokerfederation_type"      : brokerfederation_type,
                "hardwaremapping_type"       : hardwaremapping_type,
             }


hexdigits = ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']


def isHash(hash):
    """
    Tests if the string provided (hash) is a 40 character hexidecimal str.
    """
    if type(hash) is not str:
        return False 
    return set(hash.lower()).issubset(hexdigits)


class ExchangeManagementError(Exception):
    """
    An exception class for the Exchange Management system.
    """


class ServiceHelper:
    """
    ServiceHelper provides a wrapper to the various service oriented
    calls of the ExchangeManagementService.  The wrapper hides the
    ResourceClient and MessageClient from the business logic of the
    EMS.  This helps clarify code.
    """
    def __init__(self, proc):
        self.proc = proc
        self.rc = ResourceClient(proc=proc)
        self.mc = MessageClient(proc=proc)

    
    def check_request(self, msg, type):
        """
        Performs basic type checking.  This should be invoked for any
        ExchangeManagementClient call that receives a specific Google 
        Buffer Protocol object.
        """
        if msg.MessageType != resource_request_type:
            raise ExchangeManagementError('wrong message type: %s' % str(msg.MessageType))
        if msg.HasField('resource_reference'):
            raise ExchangeManagementError('resource_reference field expected to be unset, received: %s' % msg.resource_reference)
        
    
    @defer.inlineCallbacks    
    def create_object(self, msg, name, description):
        """
        Creates a ResourceManagement object based on the the parameters
        provided.
        """
        object = msg.configuration
        type = object.ObjectType
        object = yield self.rc.create_instance(type, name, description)
        yield defer.returnValue(object)
        
        
    @defer.inlineCallbacks    
    def create_object_by_id(self, type, name, description):
        """
        Creates a ResourceManagement object based on the the parameters
        provided.
        """
        object = yield self.rc.create_instance(type, name, description)
        yield defer.returnValue(object)
        
                
    @defer.inlineCallbacks    
    def push_object(self, object):
        """
        Pushes a newly created ResourceManagement object to the data
        store.
        """
        yield self.rc.put_instance(object)
        response = yield self.mc.create_instance(object.ResourceType, name='create_instrument_resource response')
        response.resource_reference = self.rc.reference_instance(object)
        response.configuration = object.ResourceObject
        response.result = 'Created'
        yield defer.returnValue(response)

    
    @defer.inlineCallbacks    
    def get_object(self, id):
        """
        Creates a ResourceManagement object based on the the parameters
        provided.
        """
        object = yield self.rc.get_instance(id)
        response = yield self.mc.create_instance(object.ResourceType, name='get_object response')
        response.resource_reference = self.rc.reference_instance(object)
        response.configuration = object.ResourceObject
        yield defer.returnValue(response)
        
        
        
class ClientHelper:
    """
    ClientHelper provides a wrapper which facilitates some of the 
    ExchangeManagementClient code.  The MessageClient is hidden 
    within this wrapper.
    """
    def __init__(self, proc):
        self.proc = proc
        self.rc = ResourceClient(proc=proc)
        self.mc = MessageClient(proc=proc)
    
    @defer.inlineCallbacks    
    def create_object(self, type):
        msg = yield self.mc.create_instance(resource_request_type, name='create_object')
        msg.configuration = msg.CreateObject(type)        
        defer.returnValue(msg)