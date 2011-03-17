#!/usr/bin/env python

"""
@file ion/play/hello_resource.py
@author David Stuebe
@brief An example service definition that can be used as template for resource management.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.messaging.message_client import MessageClient
from ion.core.exception import ApplicationError

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

from ion.core.object import object_utils

# from net.ooici.play instrument_example.proto
INSTRUMENT_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=20029, version=1)
"""
package net.ooici.play;


message InstrumentResource{
    enum _MessageTypeIdentifier {
      _ID = 20029;
      _VERSION = 1;
    }
    optional string name = 1;
    optional string make = 2;
    optional string model = 3;
    optional string serial_number = 4;
}

"""
# from net.ooici.play instrument_example.proto

INSTRUMENT_INFO_TYPE = object_utils.create_type_identifier(object_id=20030, version=1)
"""

message InstrumentInfoObject{
    enum _MessageTypeIdentifier {
      _ID = 20030;
      _VERSION = 1;
    }
    optional string name = 1;
    optional string make = 2;
    optional string model = 3;
    optional string serial_number = 4;
}
"""

RESOURCE_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
"""
package net.ooici.core.message;

import "net/ooici/core/link/link.proto";

message ResourceConfigurationRequest{
    enum _MessageTypeIdentifier {
      _ID = 10;
      _VERSION = 1;
    }
    
    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
    
    enum LifeCycleOperation {
	Activate=1;
	Deactivate=2;
	Commission=3;
	Decommission=4;
	Retire=5;
	Develope=6;
    }
    
    optional LifeCycleOperation life_cycle_operation = 3;
    
}

"""

RESOURCE_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)
"""
message ResourceConfigurationResponse{
    enum _MessageTypeIdentifier {
      _ID = 12;
      _VERSION = 1;
    }
    
    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
    
    optional string result = 3;
}
"""

RESOURCE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=4, version=1)
"""
package net.ooici.core.link;

import "net/ooici/core/type/type.proto";

message CASRef {
    enum _MessageTypeIdentifier {
        _ID = 3;
        _VERSION = 1;
    }
	required bytes key = 1;
	required net.ooici.core.type.GPBType type = 2;
	required bool isleaf = 3;
}

message IDRef {
    enum _MessageTypeIdentifier {
        _ID = 4;
        _VERSION = 1;
    }
	required string key = 1;
	//required net.ooici.core.type.GPBType type = 2;
	optional string branch = 3;
	optional string commit = 4;
}
"""

class HelloResourceError(ApplicationError):
    """
    An exception class for the Hello Resource example
    """


class HelloResource(ServiceProcess):
    """
    Example service which manages a set of resources.

    In this example we have defined a Request and Response type which may generally be useful, but it is not a
    canonical message type. It provides a common pattern for working with resources.
    
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_resource',
                                             version='0.1.0',
                                             dependencies=[])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        
        # Can be called in __init__ or in slc_init... no yield required
        self.rc = ResourceClient(proc=self)
        
        self.mc = MessageClient(proc=self)
        
        self.instance_counter = 1
        
        log.info('SLC_INIT HelloResource')

    @defer.inlineCallbacks
    def op_create_instrument_resource(self, request, headers, msg):
        """
        @Brief This method assumes that the caller provides an Instrument Info Object
        in a Resource Configuration Request message which should be made into a
        resource.

        @param params request GPB, 10/1, a request to operate on a resource
        @retval response, GPB 12/1, a response from the service which handles the resource
        """
        log.info('op_create_instrument_resource: ')

        # Check only the type received and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != RESOURCE_REQUEST_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST)
            
        ### Check the type of the configuration request
        if request.IsFieldSet('resource_reference'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with NO resource_reference field, received an illegal message!',
                                     request.ResponseCodes.BAD_REQUEST)
            
        # Attributes of a resource like name and description should be controlled
        # by the service that manages them
        name = 'Insturment #%d' % self.instance_counter
        self.instance_counter+=1
        
        # Use the resource client to create a resource!
        resource = yield self.rc.create_instance(INSTRUMENT_RESOURCE_TYPE, ResourceName=name, ResourceDescription='Preposterous instrument resource!')
        
        ### Set any fields provided by the configuration request
        if request.IsFieldSet('configuration'):
            
            ### Check the type of the configuration request
            if request.configuration.ObjectType != INSTRUMENT_INFO_TYPE:
                # This will terminate the hello service. As an alternative reply okay with an error message
                raise HelloResourceError('Expected message type InstrumentInfoRequest, received %s; type %s'
                                         % (str(request.configuration), str(request.configuration.ObjectClass)),
                                         request.ResponseCodes.BAD_REQUEST)
                
            if request.configuration.IsFieldSet('name'):
                resource.name = request.configuration.name
                
            if request.configuration.IsFieldSet('make'):
                resource.make = request.configuration.make
                
            if request.configuration.IsFieldSet('model'):
                resource.model = request.configuration.model
                
            if request.configuration.IsFieldSet('serial_number'):
                resource.serial_number = request.configuration.serial_number
        
        
        yield self.rc.put_instance(resource)
        
        log.info(str(resource))
        
        
        response = yield self.mc.create_instance(MessageContentTypeID = RESOURCE_RESPONSE_TYPE)
        
        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller
        
        # pass the reference
        response.resource_reference = self.rc.reference_instance(resource)
        
        # pass the current configuration
        response.configuration = resource.ResourceObject

        # Set a response code in the message envelope
        response.MessageResponseCode = response.ResponseCodes.OK
                
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_update_instrument_resource(self, request, headers, msg):
        """
        @Brief This method is an example using a standard resource update request.
        It contains two parts, the reference to the resource to update and the
        state of the resource which should be set.

        @param params request GPB, 10/1, a request to operate on a resource
        @retval response, GPB 12/1, a response from the service which handles the resource
        """
        
        log.info('op_update_instrument_resource: ')
            
        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != RESOURCE_REQUEST_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST )
        
        ### Check the type of the configuration request
        if not request.IsFieldSet('configuration'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with configuration field of type InstrumentInfoRequest, received empty configuration!')

        if request.configuration.ObjectType != INSTRUMENT_INFO_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with configuration field of type InstrumentInfoRequest, received %s; type %s'
                    % (str(request.configuration), str(request.configuration.ObjectClass)),
                                     request.ResponseCodes.BAD_REQUEST)

        ### Check the type of the configuration request
        if not request.IsFieldSet('resource_reference'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with resource_reference field of type IDRef, received empty resource_reference!',
                                     request.ResponseCodes.BAD_REQUEST)
            
        if request.resource_reference.ObjectType != RESOURCE_REFERENCE_TYPE:

            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with resource_reference field of type IDRef, received %s; type %s'
                    % (str(request.resource_reference), str(request.resource_reference.ObjectClass)),
                                     request.ResponseCodes.BAD_REQUEST)


            
        # Get the current state of the resource
        resource = yield self.rc.get_instance(request.resource_reference)
        
        ### Set any fields provided by the configuration request
        if request.IsFieldSet('configuration'):

            ### Check the type of the configuration request
            
            # This method will set the field in the resource if it is set in the message.
            #if request.configuration.IsFieldSet('name'):
            #    resource.name = request.configuration.name
            #    
            #if request.configuration.IsFieldSet('make'):
            #    resource.make = request.configuration.make
            #    
            #if request.configuration.IsFieldSet('model'):
            #    resource.model = request.configuration.model
            #    
            #if request.configuration.IsFieldSet('serial_number'):
            #    resource.serial_number = request.configuration.serial_number
        
            ### This method will clear the field in the resource if it is not set in the message.
            if request.configuration.IsFieldSet('name'):
                resource.name = request.configuration.name
            else:
                resource.ClearField('name')
                
            if request.configuration.IsFieldSet('make'):
                resource.make = request.configuration.make
            else:
                resource.ClearField('make')
                
            if request.configuration.IsFieldSet('model'):
                resource.model = request.configuration.model
            else:
                resource.ClearField('model')
                
            if request.configuration.IsFieldSet('serial_number'):
                resource.serial_number = request.configuration.serial_number
            else:
                resource.ClearField('serial_number')
        
        yield self.rc.put_instance(resource)
        
        log.info(str(resource))
        
        
        response = yield self.mc.create_instance(MessageContentTypeID = RESOURCE_RESPONSE_TYPE)
        
        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller
        
        # pass the reference
        response.resource_reference = self.rc.reference_instance(resource)
        
        # pass the current configuration
        response.configuration = resource.ResourceObject
        
        response.MessageResponseCode = response.ResponseCodes.OK
                
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)
        

    @defer.inlineCallbacks
    def op_set_instrument_resource_life_cycle(self, request, headers, msg):
        """
        This method is an example using a standard resource update request.
        It contains two parts, the reference to the resource to update and the
        state of the resource which should be set.

        @param params request GPB, 10/1, a request to operate on a resource
        @retval simple ack message
        """
        
        log.info('op_set_instrument_resource_life_cycle: ')
            
        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != RESOURCE_REQUEST_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST)
        
        ### Check the type of the configuration request
        if request.IsFieldSet('configuration'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with NO configuration field, received an illegal message!',
                                     request.ResponseCodes.BAD_REQUEST)
        
        ### Check the type of the configuration request
        if not request.IsFieldSet('resource_reference'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with resource_reference field of type IDRef, received empty configuration!',
                                     request.ResponseCodes.BAD_REQUEST)
            
        if request.resource_reference.ObjectType != RESOURCE_REFERENCE_TYPE:

            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with resource_reference field of type IDRef, received %s; type %s'
                    % (str(request.resource_reference), str(request.resource_reference.ObjectClass)),
                                     request.ResponseCodes.BAD_REQUEST)

        if not request.IsFieldSet('life_cycle_operation'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with a life_cycle_operation field, received an illegal message!',
                                     request.ResponseCodes.BAD_REQUEST)
            ### Don't need to check the type of the lco field... it is gpb defined
            
        # Get the current state of the resource
        resource = yield self.rc.get_instance(request.resource_reference)
        
        
        # Business logic to modify physical resources goes inside these if statements!
        if request.life_cycle_operation == request.MessageObject.LifeCycleOperation.ACTIVATE:
           resource.ResourceLifeCycleState = resource.ACTIVE

        elif request.life_cycle_operation == request.MessageObject.LifeCycleOperation.DEACTIVATE:
           resource.ResourceLifeCycleState = resource.INACTIVE
           
        elif request.life_cycle_operation == request.MessageObject.LifeCycleOperation.COMMISSION:
           resource.ResourceLifeCycleState = resource.COMMISSIONED
           
        elif request.life_cycle_operation == request.MessageObject.LifeCycleOperation.Decommission:
           resource.ResourceLifeCycleState = resource.DECOMMISSIONED
           
        elif request.life_cycle_operation == request.MessageObject.LifeCycleOperation.Retire:
           resource.ResourceLifeCycleState = resource.RETIRED
           
        elif request.life_cycle_operation == request.MessageObject.LifeCycleOperation.Develop:
           resource.ResourceLifeCycleState = resource.DEVELOPED

        yield self.rc.put_instance(resource)
        
        # Just reply ok...
        yield self.reply_ok(msg)
        

class HelloResourceClient(ServiceClient):
    """
    This is an exemplar service client that calls the hello object service. It
    makes service calls RPC style using GPB object. There is no special handling
    here, just call send. The clients should become exteremly thin wrappers with
    no business logic.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_resource"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def create_instrument_resource(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('create_instrument_resource', msg)
        
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def update_instrument_resource(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('update_instrument_resource', msg)
        
        defer.returnValue(content)

    @defer.inlineCallbacks
    def set_instrument_resource_life_cycle(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('set_instrument_resource_life_cycle', msg)
        
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(HelloResource)


