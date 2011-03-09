#!/usr/bin/env python

"""
@file ion/services/dm/inventory/dataset_controller.py
@author David Stuebe
@brief An example service definition that can be used as template for resource management.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.exception import ApplicationError

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

from ion.core.object import object_utils

CMD_DATASET_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=10001, version=1)
"""
message Dataset {
   enum _MessageTypeIdentifier {
        _ID = 10001;
        _VERSION = 1;
    }
   optional net.ooici.core.link.CASRef root_group = 1;
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

FindDatasetRequest_TYPE = object_utils.create_type_identifier(object_id=2401, version=1)
"""
message FindDatasetMessage {
    enum _MessageTypeIdentifier {
		_ID = 2401;
		_VERSION = 1;
	}

    optional bool only_mine = 1 ;
    optional net.ooici.services.coi.LifeCycleState by_life_cycle_State = 2 [default = ACTIVE];
    }
"""

ListFindResults_TYPE = object_utils.create_type_identifier(object_id=22, version=1)
"""
message QueryResult{
    enum _MessageTypeIdentifier {
      _ID = 22;
      _VERSION = 1;
    }
    repeated net.ooici.core.link.CASRef idref = 1;
}
"""



class DatasetControllerError(ApplicationError):
    """
    An exception class for the Dataset Controller Service
    """


class DatasetController(ServiceProcess):
    """
    The Dataset Controller service

    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='dataset_controller',
                                             version='0.1.0',
                                             dependencies=[])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.

        # Can be called in __init__ or in slc_init... no yield required
        self.resource_client = ResourceClient(proc=self)

        log.info('SLC_INIT Dataset Controller')

    @defer.inlineCallbacks
    def op_create_dataset_resource(self, request, headers, msg):
        """
        @Brief This method assumes that the caller provides an Instrument Info Object
        in a Resource Configuration Request message which should be made into a
        resource.

        @param params request GPB, ?, Is there anything in the request? What?
        @retval response, GPB 12/1, a response containing the dataset resource ID
        """
        log.info('op_create_dataset_resource: ')

        # Check only the type received and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != RESOURCE_REQUEST_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST)

        ### Check the type of the configuration request
        if request.IsFieldSet('resource_reference'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message with NO resource_reference field, received an illegal message!',
                                     request.ResponseCodes.BAD_REQUEST)

        # Attributes of a resource like name and description should be controlled
        # by the service that manages them

        # Use the resource client to create a resource!
        resource = yield self.resource_client.create_instance(CMD_DATASET_RESOURCE_TYPE,
                                                              ResourceName='dataset',
                                                              ResourceDescription='Preposterous instrument resource!')

        ###
        ### Do any required setup for the new resource object?
        ###




        ###
        ### Call pubsub controller to create a topic for the dataset
        ###

        ###
        ### Call the pubsub controller to create a binding for the ingestion service to the topic.
        ###


        ###
        ### Create an association between the topic and the dataset?
        ###


        resource.ResourceLifeCycleState = resource.DEVELOPED

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
    def op_update_dataset_resource(self, request, headers, msg):
        """
        @Brief What does this mean for a dataset - the ingestion service handles ingesting supplemental data!

        @param params request GPB, 10/1, a request to operate on a resource
        @retval response, GPB 12/1, a response from the service which handles the resource
        """




    @defer.inlineCallbacks
    def op_set_dataset_resource_life_cycle(self, request, headers, msg):
        """
        @Brief set the lifecycle state of the dataset resource

        @param params request GPB, 10/1, a request to operate on a resource
        @retval simple ack message
        """

        log.info('op_set_dataset_resource_life_cycle: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != RESOURCE_REQUEST_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST)

        ### Check the type of the configuration request
        if request.IsFieldSet('configuration'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message with NO configuration field, received an illegal message!',
                                     request.ResponseCodes.BAD_REQUEST)

        ### Check the type of the configuration request
        if not request.IsFieldSet('resource_reference'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message with resource_reference field of type IDRef, received empty configuration!',
                                     request.ResponseCodes.BAD_REQUEST)

        if request.resource_reference.ObjectType != RESOURCE_REFERENCE_TYPE:

            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message with resource_reference field of type IDRef, received %s; type %s'
                    % (str(request.resource_reference), str(request.resource_reference.ObjectClass)),
                                     request.ResponseCodes.BAD_REQUEST)

        if not request.IsFieldSet('life_cycle_operation'):
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message with a life_cycle_operation field, received an illegal message!',
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



    @defer.inlineCallbacks
    def op_find_dataset_resources(self, request, headers, msg):
        """
        @Brief set the lifecycle state of the dataset resource

        @param params request GPB, 2401/1, a request to find datasets
        @retval ListFindResults Type, GPB 22/1, A list of Dataset Resource References that match the request
        """

        # Request params:
        #   By Owner
        #   Assume LCS Active unless specified
        #   By Name - What is name!!!

        # Assumed - By type datast

        # Return - a list of dataset resources!




class DatasetControllerClient(ServiceClient):
    """
    Dataset Controller Svc Client
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "dataset_controller"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def create_dataset_resource(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('create_dataset_resource', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def update_dataset_resource(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('update_dataset_resource', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def set_dataset_resource_life_cycle(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('set_dataset_resource_life_cycle', msg)

        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(DatasetController)


