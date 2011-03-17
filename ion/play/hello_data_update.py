#!/usr/bin/env python

"""
@file ion/play/hello_data_update.py
@author David Stuebe
@brief An example service definition that can be used as template for data
resource management. This is a more complex example of resource management
specific to datasets...
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

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)
"""
package net.ooici.play;

// Copied from the google example!
// Changed rules - never use required!

import "net/ooici/core/link/link.proto";

message Person {
  enum _MessageTypeIdentifier {
    _ID = 20001;
    _VERSION = 1;
  }
  optional string name = 1;
  optional int32 id = 2;        // Unique ID number for this person.
  optional string email = 3;

  enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
  }

  message PhoneNumber {
    optional string number = 1;
    optional PhoneType type = 2 [default = HOME];
  }

  repeated PhoneNumber phone = 4;
}

// Our address book file is just one of these.
message AddressBook {
  enum _MessageTypeIdentifier {
    _ID = 20002;
    _VERSION = 1;
  }
  repeated Person person = 1;
  optional Person owner = 2;
  optional string title = 3;
}


// Our address book file is just one of these.
message AddressLink {
  enum _MessageTypeIdentifier {
    _ID = 20003;
    _VERSION = 1;
  }
  repeated net.ooici.core.link.CASRef person = 1;
  optional net.ooici.core.link.CASRef owner = 2;
  optional string title = 3;
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



class HelloDataUpdateError(Exception):
    """
    An exception class for the Hello Object example
    """


class HelloDataUpdate(ServiceProcess):
    """
    Example service which manages a set of resources.
    
    It is possible to make fewer "op_'s" and send more complex arguments.
    Here I have choosen to break out the various operations to demonstrate each
    function more clearly. It is up to the developer and the architecture team
    whether there one 'update' operation and the mode (clobber, merge, append) is
    sent as part of the message object, or like in this example, there are
    multiple operations one for each mode.
    
    Similarly this example shows an service which 'loosly' controls its resources.
    The caller is has access to the resources by reference. With a resource client
    the caller could just as easily make changes directly.
    
    The developer may wish to abstract the calls to the service and never expose
    the actual resource references to the caller.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_data_update',
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
    def op_create_addressbook_resource(self, request, headers, msg):
        """
        This method assumes that the caller provides an addresslink object which
        should be made into a resource This is not the standard pattern for
        resource and is likely only to be used for data resources. 
        """
        log.info('op_create_addressbook_resource: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != resource_request_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloDataUpdateError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request))
            
        # Attributes of a resource like name and description should be controlled
        # by the service that manages them
        name = 'Addressbook #%d' % self.instance_counter
        self.instance_counter+=1
        
        resource = yield self.rc.create_instance(addresslink_type, ResourceName=name, ResourceDescription='Preposterous names!')
        
        # the ResourceObject property of the ResourceInstance provides a setter
        # for the value of the entire resource object - it is usefull for creation
        # and clobber updates...
        resource.ResourceObject = request.configuration
        
        yield self.rc.put_instance(resource)
        
        log.info(str(resource))
        
        
        response = yield self.mc.create_instance(resource_response_type, MessageName='create_addressbook_resource response')
        
        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller        
        response.resource_reference = self.rc.reference_instance(resource)
        response.configuration = resource.ResourceObject
        response.result = 'Created'
        
        
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_clobber_addressbook_resource(self, request, headers, msg):
        """
        This method is an example using a standard resource update request.
        It contains two parts, the reference to the resource to update and the
        state of the resource which should be set.
        """
        
        log.info('op_clobber_addressbook_resource: ')
            
        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != resource_request_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloDataUpdateError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request))
            
        # Get the current state of the resource
        resource = yield self.rc.get_instance(request.resource_reference)
        
        # Merge the requested state into the resource history
        yield resource.MergeResourceUpdate(resource.CLOBBER, request.configuration)
            
        # Clobber the current state with the update
        resource.ResourceObject = resource.CompareToUpdates[0]
        # resource.ResourceObject is a property setter/getter for the resource object
        # resource.CompareToUpdates is a getter for the list of updated states that are being merged
            
        yield self.rc.put_instance(resource)
            
        response = yield self.mc.create_instance(resource_response_type, MessageName='clobber_addressbook_resource response')
        
        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller        
        response.resource_reference = self.rc.reference_instance(resource)
        response.configuration = resource.ResourceObject
        response.result = 'Clobbered'
         
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_merge_addressbook_resource(self, request, headers, msg):
        """
        This method is an example using a standard resource update request.
        It contains two parts, the reference to the resource to update and the
        state of the resource which should be set.
        """
        
        log.info('op_clobber_addressbook_resource: ')
            
        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != resource_request_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloResourceError('Expected message class ResourceConfigurationRequest, received %s'
                                     % str(request))
            
        # Get the current state of the resource
        resource = yield self.rc.get_instance(request.resource_reference)
        
        # Merge the requested state into the resource history
        yield resource.MergeResourceUpdate(resource.MERGE, request.configuration)
            
        # Merge the current state with the update
        
        # Compare and resolve differences...
        if resource.title != resource.CompareToUpdates[0].title:
            # resolve a difference in a string field...
            resource.title = resource.CompareToUpdates[0].title
            
        if resource.owner != resource.CompareToUpdates[0].owner:
            # resolve a difference in a object field
            resource.owner = resource.CompareToUpdates[0].owner

        # merge something more complex...
        for updated_person in resource.CompareToUpdates[0].person:
            # Resolve a difference between two lists of objects based on their ID #
            for idx, person in zip(range(len(resource.person)), resource.person):
                
                if updated_person.id == person.id:
                    resource.person[idx] = updated_person
                    break
            
            else:
                new_person = resource.person.add()
                new_person.SetLink(updated_person)
                    
            
            
        yield self.rc.put_instance(resource)
            
        response = yield self.mc.create_instance(resource_response_type, MessageName='clobber_addressbook_resource response')
        
        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller        
        response.resource_reference = self.rc.reference_instance(resource)
        response.configuration = resource.ResourceObject
        response.result = 'Merged'
            
        yield self.reply_ok(msg, response)


class HelloDataUpdateClient(ServiceClient):
    """
    This is an exemplar service client that calls the hello update data service.
    It makes service calls RPC style using GPB objects. There is no special handling
    here, just call send. The clients should become exteremly thin wrappers with
    no business logic.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_data_update"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def create_addressbook_resource(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('create_addressbook_resource', msg)
        
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def clobber_addressbook_resource(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('clobber_addressbook_resource', msg)
        
        defer.returnValue(content)

    @defer.inlineCallbacks
    def merge_addressbook_resource(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('clobber_addressbook_resource', msg)
        
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(HelloDataUpdate)


