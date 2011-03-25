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

IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)
"""
message IDRef {
    enum _MessageTypeIdentifier {
        _ID = 4;
        _VERSION = 1;
    }
	required string key = 1;
	optional string branch = 3;
	optional bytes commit = 4;
}
"""

FINDDATASETREQUEST_TYPE = object_utils.create_type_identifier(object_id=2401, version=1)
"""
message FindDatasetMessage {
    enum _MessageTypeIdentifier {
		_ID = 2401;
		_VERSION = 1;
	}

    optional bool only_mine = 1 ;
    optional net.ooici.services.coi.LifeCycleState by_life_cycle_state = 2 [default = ACTIVE];
    }
"""

QUERYRESULTS_TYPE = object_utils.create_type_identifier(object_id=22, version=1)
"""
message QueryResult{
    enum _MessageTypeIdentifier {
      _ID = 22;
      _VERSION = 1;
    }
    repeated net.ooici.core.link.CASRef idrefs = 1;
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

        self.dataset_dict = {}

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
        if request.MessageType != None:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message type ResourceConfigurationRequest, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST)

        # Use the resource client to create a resource!
        resource = yield self.resource_client.create_instance(CMD_DATASET_RESOURCE_TYPE,
                                                              ResourceName='CDM Dataset Resource',
                                                              ResourceDescription='None')

        # What state should this be in at this point?
        #resource.ResourceLifeCycleState = resource.DEVELOPED
        #yield self.rc.put_instance(resource)

        log.info(str(resource))

        # Temporary - add to list of datasets...
        self.dataset_dict[resource.ResourceIdentity] = self.resource_client.reference_instance(resource)


        response = yield self.message_client.create_instance(MessageContentTypeID = IDREF_TYPE)

        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller

        # pass the reference
        response.MessageObject = self.resource_client.reference_instance(resource)

        # Set a response code in the message envelope
        response.MessageResponseCode = response.ResponseCodes.OK
        
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_find_dataset_resources(self, request, headers, msg):
        """
        @Brief set the lifecycle state of the dataset resource

        @param params request GPB, 2401/1, a request to find datasets
        @retval ListFindResults Type, GPB 22/1, A list of Dataset Resource References that match the request
        """


        log.info('op_set_dataset_resource_life_cycle: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != FINDDATASETREQUEST_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message type FindDataSetRequest, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST)

        ### Check the type of the configuration request
        if request.IsFieldSet('only_mine'):
           pass

        ### Check the type of the configuration request
        if request.IsFieldSet('by_life_cycle_state'):
            pass

        # No acutal find yet - just return whatever items we have...

        response = yield self.message_client.create_instance(MessageContentTypeID = QUERYRESULTS_TYPE)


        for item in self.dataset_dict.values():
            link = response.idrefs.add()
            link.SetLink(item)

        self.reply_ok(msg, response)



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
    def set_dataset_resource_life_cycle(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('set_dataset_resource_life_cycle', msg)

        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(DatasetController)


