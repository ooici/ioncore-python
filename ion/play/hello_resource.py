#!/usr/bin/env python

"""
@file ion/play/hello_resource.py
@author David Stuebe
@brief An example service definition that can be used as template for resource management.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.messaging.message_client import MessageClient


from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.services.coi.resource_registry_beta.resource_client import ResourceClientError, ResourceInstanceError

from ion.core.object import object_utils

instrument_resource_type = object_utils.create_type_identifier(object_id=20029, version=1)
instrument_info_type = object_utils.create_type_identifier(object_id=20030, version=1)
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

resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)
resource_response_type = object_utils.create_type_identifier(object_id=12, version=1)
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
resource_reference_type = object_utils.create_type_identifier(object_id=4, version=1)
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

class HelloResourceError(Exception):
    """
    An exception class for the Hello Resource example
    """


class HelloResource(ServiceProcess):
    """
    Example service which manages a set of resources.
    
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
        This method assumes that the caller provides an Instrument Info Object
        in a Resource Configuration Request message which should be made into a
        resource.
        
        This is the standard pattern for working with a resource.
        
        """
        log.info('op_create_instrument_resource: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != resource_request_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloDataUpdateError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request))
            
        ### Check the type of the configuration request
        if request.IsFieldSet('resource_reference'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with NO resource_reference field, received an illegal message!')
            
        # Attributes of a resource like name and description should be controlled
        # by the service that manages them
        name = 'Insturment #%d' % self.instance_counter
        self.instance_counter+=1
        
        # Use the resource client to create a resource!
        resource = yield self.rc.create_instance(instrument_resource_type, name=name, description='Preposterous instrument resource!')
        
        ### Set any fields provided by the configuration request
        if request.IsFieldSet('configuration'):
            
            ### Check the type of the configuration request
            if request.configuration.ObjectType != instrument_info_type:
                # This will terminate the hello service. As an alternative reply okay with an error message
                raise HelloResourceError('Expected message type InstrumentInfoRequest, received %s; type %s'
                                         % (str(request.configuration), str(request.configuration.ObjectClass)))
                
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
        
        
        response = yield self.mc.create_instance(resource_response_type, name='create_instrument_resource response')
        
        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller
        
        # pass the reference
        response.resource_reference = self.rc.reference_instance(resource)
        
        # pass the current configuration
        response.configuration = resource.ResourceObject
        
        # pass the result of the create operation...
        response.result = 'Created'
                
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_update_instrument_resource(self, request, headers, msg):
        """
        This method is an example using a standard resource update request.
        It contains two parts, the reference to the resource to update and the
        state of the resource which should be set.
        """
        
        log.info('op_update_instrument_resource: ')
            
        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != resource_request_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request))
        
        ### Check the type of the configuration request
        if not request.IsFieldSet('configuration'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with configuration field of type InstrumentInfoRequest, received empty configuration!')

        if request.configuration.ObjectType != instrument_info_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with configuration field of type InstrumentInfoRequest, received %s; type %s'
                    % (str(request.configuration), str(request.configuration.ObjectClass)))

        ### Check the type of the configuration request
        if not request.IsFieldSet('resource_reference'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with resource_reference field of type IDRef, received empty resource_reference!')
            
        if request.resource_reference.ObjectType != resource_reference_type:

            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with resource_reference field of type IDRef, received %s; type %s'
                    % (str(request.resource_reference), str(request.resource_reference.ObjectClass)))


            
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
        
        
        response = yield self.mc.create_instance(resource_response_type, name='update_instrument_resource response')
        
        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller
        
        # pass the reference
        response.resource_reference = self.rc.reference_instance(resource)
        
        # pass the current configuration
        response.configuration = resource.ResourceObject
        
        # pass the result of the create operation...
        response.result = 'Created'
                
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)
        

    @defer.inlineCallbacks
    def op_set_instrument_resource_life_cycle(self, request, headers, msg):
        """
        This method is an example using a standard resource update request.
        It contains two parts, the reference to the resource to update and the
        state of the resource which should be set.
        """
        
        log.info('op_set_instrument_resource_life_cycle: ')
            
        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != resource_request_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request))
        
        ### Check the type of the configuration request
        if request.IsFieldSet('configuration'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with NO configuration field, received an illegal message!')
        
        ### Check the type of the configuration request
        if not request.IsFieldSet('resource_reference'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with resource_reference field of type IDRef, received empty configuration!')
            
        if request.resource_reference.ObjectType != resource_reference_type:

            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with resource_reference field of type IDRef, received %s; type %s'
                    % (str(request.resource_reference), str(request.resource_reference.ObjectClass)))

        if not request.IsFieldSet('life_cycle_operation'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message with a life_cycle_operation field, received an illegal message!')
            ### Don't need to check the type of the lco field... it is gpb defined
            
        # Get the current state of the resource
        resource = yield self.rc.get_instance(request.resource_reference)
        
        
        # Business logic to modify physical resources goes inside these if statements!
        if request.life_cycle_operation == request.MessageObject.Activate:
           resource.ResourceLifeCycleState = resource.ACTIVE

        elif request.life_cycle_operation == request.MessageObject.Deactivate:
           resource.ResourceLifeCycleState = resource.INACTIVE
           
        elif request.life_cycle_operation == request.MessageObject.Commission:
           resource.ResourceLifeCycleState = resource.COMMISSIONED
           
        elif request.life_cycle_operation == request.MessageObject.Decommission:
           resource.ResourceLifeCycleState = resource.DECOMMISSIONED
           
        elif request.life_cycle_operation == request.MessageObject.Retire:
           resource.ResourceLifeCycleState = resource.RETIRED
           
        elif request.life_cycle_operation == request.MessageObject.Develop:
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


