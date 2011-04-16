#!/usr/bin/env python

"""
@file ion/integration/ais/createDataResource/createDataResource.py
@author Ian Katz
@brief The worker class that implements the createDataResource function for the AIS  (workflow #105)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.messaging.message_client import MessageClient
from ion.services.dm.inventory import DatasetControllerClient
from ion.services.dm.ingestion.eoi_ingester import EOIIngestionClient

from ion.core.exception import ReceivedApplicationError, ReceivedContainerError

from ion.services.coi.resource_registry_beta.association_client import AssociationClient
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, \
                                                                    ResourceInstance
from ion.services.coi.resource_registry_beta.resource_client import ResourceClientError, \
                                                                    ResourceInstanceError

from ion.core.object import object_utils

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       OOI_ID_TYPE, \
                                                       CREATE_DATA_RESOURCE_REQ_TYPE, \
                                                       CREATE_DATA_RESOURCE_RSP_TYPE, \
                                                       CREATE_DATA_RESOURCE_SIMPLE_REQ_TYPE


RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
"""
from ion-object-definitions/net/ooici/core/message/resource_request.proto
message ResourceConfigurationRequest{
    enum _MessageTypeIdentifier {
      _ID = 10;
      _VERSION = 1;
    }

    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
"""

RESOURCE_CFG_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)
"""
from ion-object-definitions/net/ooici/core/message/resource_request.proto
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

SA_DATASOURCE_RESOURCE_MSG = object_utils.create_type_identifier(object_id=4503, version=1)
"""
message DataSourceResource {
    enum _MessageTypeIdentifier {
        _ID = 4503;
        _VERSION = 1;
    }

    // Contains information required by the EOI Dataset Agent
    // to properly obtain data from an external source
    //
    // For more information see:
    // https://spreadsheets.google.com/ccc?key=tql6nRd7fM6gSxY0KnBMzrA&authkey=CLWPmZAJ&hl=en#gid=0

    optional SourceType source_type    = 1;
    repeated string property           = 2;
    repeated string station_id         = 3;

    optional RequestType request_type  = 4;
    optional double top                = 5;
    optional double bottom             = 6;
    optional double left               = 7;
    optional double right              = 8;
    optional string base_url           = 9;
    optional string dataset_url        = 10;
    optional string ncml_mask          = 11;
    optional uint64 max_ingest_millis  = 12;

    //'start_time' and 'end_time' are expected to be in the
    // ISO8601 Date Format (yyyy-MM-dd'T'HH:mm:ss'Z')
    optional string start_time         = 13;
    optional string end_time           = 14;
    optional string institution_id     = 15;
}

"""


SCHEDULER_ADD_REQ_TYPE = object_utils.create_type_identifier(object_id=2601, version=1)
"""
message AddTaskRequest {
    enum _MessageTypeIdentifier {
      _ID = 2601;
      _VERSION = 1;
    }

    // desired_origin is where the event notification will originate from
    //   this is not required to be sent... one will be generated if not
    // interval is seconds between messages
    // payload is string

    optional string desired_origin    = 1;
    optional uint64 interval_seconds  = 2;
    optional string payload           = 3;

}
"""

SCHEDULER_ADD_RSP_TYPE = object_utils.create_type_identifier(object_id=2602, version=1)
"""
message AddTaskResponse {
    enum _MessageTypeIdentifier {
      _ID = 2602;
      _VERSION = 1;
    }

    // the string guid
    // the origin  is where the event notifications will come from

    optional string task_id = 1;
    optional string origin  = 2;
}
"""


class CreateDataResource(object):

    def __init__(self, ais):
        log.debug('CreateDataResource.__init__()')
        self.mc    = ais.mc
        self.rc    = ais.rc
        self.dscc  = DatasetControllerClient(proc=ais)
        self.psc   = PubSubClient(proc=ais)
        self.ac    = AssociationClient(proc=ais)

    @defer.inlineCallbacks
    def createDataResourceDap(self, msg):
        """
        @brief create a data resource based on the limited data we can receive from UX
        @param msg GPB, 9217/1,
        @GPB{Input,9217,1}
        @GPB{Returns,9212,1}
        @retval IDs of new objects, GPB 9212/1, otherwise an AIS error GPB
        """

        try:
            # Check only the type recieved and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if msg.MessageType != CREATE_DATA_RESOURCE_SIMPLE_REQ_TYPE:
                errtext = "CreateDataResource.createDataResource(): " + \
                    "Expected DataResourceCreateSimpleRequest type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                                         MessageName='AIS CreateDataResource error response')
                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)


            createfull_msg = yield self.mc.create_instance(CREATE_DATA_RESOURCE_REQ_TYPE,
                                                           ResourceName='datasource',
                                                           ResourceDescription='Important Datasource Description')

            #fill in any fields that can happen automatically
            createfull_msg.request_type = createfull_msg.RequestType.DAP
            # if that doesnt work try:
            #     createfull_msg.request_type = createfull_msg.ObjectResource.RequestType.DAP

            ret = yield self.createDataResource(createfull_msg)
            defer.returnValue(ret)


        except ReceivedApplicationError, ex:
            log.info('CreateDataResource.createDataResourceDap(): Error attempting to FIXME: %s' %ex)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                                     MessageName='AIS CreateDataResourceDap error response')
            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)



    @defer.inlineCallbacks
    def createDataResource(self, msg):
        """
        @brief create a data resource
        @param msg GPB, 9211/1,
        @GPB{Input,9211,1}
        @GPB{Returns,9212,1}
        @retval IDs of new objects, GPB 9212/1, otherwise an AIS error GPB
        """
        log.info('CreateDataResource.createDataResource()\n')

        my_datasrc_id      = None
        my_dataset_id      = None
        my_association_id  = None

        datasrc_resource      = None
        dataset_resource      = None
        association_resource  = None

        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if msg.MessageType != CREATE_DATA_RESOURCE_REQ_TYPE:
                errtext = "CreateDataResource.createDataResource(): " + \
                    "Expected DataResourceCreateRequest type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                                         MessageName='AIS CreateDataResource error response')
                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            # create the data source
            datasrc_resource = yield self._createDataSourceResource(msg)
            my_datasrc_id = datasrc_resource.ResourceIdentity

            # create the dataset
            dataset_req = yield self.mc.create_instance(None)
            dataset_resource = yield self.dscc.create_dataset_resource(dataset_req)
            log.info("created data set " + str(dataset_resrource))

            # next line could also be self.rc.reference_instance(datasrc_resource).key
            my_dataset_id = dataset_resource.key

            # FIXME create topics
            # call EOI: create_dataset_topics ( tim laroque or dave ... BLOCKED FOR NOW )

            # FIXME call the scheduler service client
            #  it returns the scheduler task id, which i'll associate with the data source
            #
            # interval_seconds=uint64, origin=string, payload
            # response is UUID + origin
            #
            # payload is the UpdateEvent, which contains the dataset id
            # target is the DS update topic



            #make association
            association = yield self.ac.create_association(dataset_resource, HAS_A_ID, datasrc_resource)
            #FIXME associate user with data source ?


            #mark lifecycle states
            datasrc_resource.ResourcesLifeCycleState = datasrc_resource.ACTIVE
            dataset_resource.ResourcesLifeCycleState = dataset_resource.ACTIVE
            yield self.rc.put_resource_transaction([datasrc_resource, dataset_resource])


        except ReceivedApplicationError, ex:
            log.info('CreateDataResource.createDataResource(): Error attempting to FIXME: %s' %ex)

            #mark lifecycle states
            datasrc_resource.ResourcesLifeCycleState = datasrc_resource.RETIRED
            dataset_resource.ResourcesLifeCycleState = dataset_resource.RETIRED
            yield self.rc.put_resource_transaction([datasrc_resource, dataset_resource])

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                                     MessageName='AIS CreateDataResource error response')
            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)



        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE,
                                                 MessageName='AIS CreateDataResource response')
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(CREATE_DATA_RESOURCE_RSP_TYPE)
        Response.message_parameters_reference[0].data_source_id  = my_datasrc_id
        Response.message_parameters_reference[0].data_set_id     = my_dataset_id
        Response.message_parameters_reference[0].association_id  = association.AssociationIdentity
        defer.returnValue(Response)


    def _createScheduledEvent(self, desired_origin, interval_seconds, payload):
        sched_resource = yield self.mc.create_instance(SA_DATASOURCE_RESOURCE_MSG,
                                                         ResourceName='datasource',
                                                         ResourceDescription='Important Datasource Description')
        
        #FILL UP FIELDS, lists followed by scalars
        datasrc_resource.property.extend(msg.property)



    @defer.inlineCallbacks
    def _createDataSourceResource(self, msg):
        """
        @brief create a data resource
        @param msg GPB, 9211/1,
        @GPB{Input,9211,1}
        @retval data source resource
        """
        log.info('CreateDataResource.createDataResource()\n')
        datasrc_resource = yield self.mc.create_instance(SA_DATASOURCE_RESOURCE_MSG,
                                                         ResourceName='datasource',
                                                         ResourceDescription='Important Datasource Description')

        #FILL UP FIELDS, lists followed by scalars
        datasrc_resource.property.extend(msg.property)
        datasrc_resource.station_id.extend(msg.station_id)

        datasrc_resource.source_type        = msg.source_type
        datasrc_resource.request_type       = msg.request_type
        datasrc_resource.top                = msg.top
        datasrc_resource.bottom             = msg.bottom
        datasrc_resource.left               = msg.left
        datasrc_resource.right              = msg.right
        datasrc_resource.base_url           = msg.base_url
        datasrc_resource.dataset_url        = msg.dataset_url
        datasrc_resource.ncml_mask          = msg.ncml_mask
        datasrc_resource.max_ingest_millis  = msg.update_interval_msec
        datasrc_resource.start_time         = msg.start_time
        datasrc_resource.end_time           = msg.end_time
        datasrc_resource.institution_id     = msg.institution_id


        #fixme, put it with the others
        yield self.rc.put_instance(datasrc_resource)
        log.info("created data source " + str(datasrc_resrource))

        defer.returnValue(datasrc_resource)
