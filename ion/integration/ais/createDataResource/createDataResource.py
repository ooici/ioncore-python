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
from ion.services.dm.inventory.dataset_controller import DatasetControllerClient
from ion.services.dm.ingestion.ingestion import IngestionClient

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
                                                       CREATE_DATA_RESOURCE_REQ_TYPE, \
                                                       CREATE_DATA_RESOURCE_RSP_TYPE, \
                                                       CREATE_DATA_RESOURCE_SIMPLE_REQ_TYPE

INGESTER_CREATETOPICS_REQ_MSG  = object_utils.create_type_identifier(object_id=2003, version=1)
RESOURCE_CFG_REQUEST_TYPE      = object_utils.create_type_identifier(object_id=10, version=1)
RESOURCE_CFG_RESPONSE_TYPE     = object_utils.create_type_identifier(object_id=12, version=1)
SA_DATASOURCE_RESOURCE_MSG     = object_utils.create_type_identifier(object_id=4503, version=1)
SCHEDULER_ADD_REQ_TYPE         = object_utils.create_type_identifier(object_id=2601, version=1)
SCHEDULER_ADD_RSP_TYPE         = object_utils.create_type_identifier(object_id=2602, version=1)



class CreateDataResource(object):

    def __init__(self, ais):
        log.debug('CreateDataResource.__init__()')
        self.mc    = ais.mc
        self.rc    = ais.rc
        self.dscc  = DatasetControllerClient(proc=ais)
        self.psc   = PubSubClient(proc=ais)
        self.ac    = AssociationClient(proc=ais)
        self.ing   = IngestionClient(proc=ais)

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
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, 1)

                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)


            createfull_msg = yield self.mc.create_instance(CREATE_DATA_RESOURCE_REQ_TYPE, 1)

            #fill in any fields that can happen automatically
            createfull_msg.request_type = createfull_msg.RequestType.DAP
            # if that doesnt work try:
            #     createfull_msg.request_type = createfull_msg.ObjectResource.RequestType.DAP

            ret = yield self.createDataResource(createfull_msg)
            defer.returnValue(ret)


        except ReceivedApplicationError, ex:
            log.info('CreateDataResource.createDataResourceDap(): Error attempting to FIXME: %s' %ex)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, 1)

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
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, 1)
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

            # create topics
            topics_msg = yield self.mc.create_instance(INGESTER_CREATETOPICS_REQ_MSG, 1)
            topics_msg.dataset_id = my_dataset_id
            self.ing.create_dataset_topics()

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

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, 1)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)



        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, 1)

        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(CREATE_DATA_RESOURCE_RSP_TYPE)
        Response.message_parameters_reference[0].data_source_id  = my_datasrc_id
        Response.message_parameters_reference[0].data_set_id     = my_dataset_id
        Response.message_parameters_reference[0].association_id  = association.AssociationIdentity
        defer.returnValue(Response)


    def _createScheduledEvent(self, desired_origin, interval_seconds, payload):
        sched_resource = yield self.mc.create_instance(SA_DATASOURCE_RESOURCE_MSG, 1)
        
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
        datasrc_resource = yield self.mc.create_instance(SA_DATASOURCE_RESOURCE_MSG, 1)

        #FILL UP FIELDS, lists followed by scalars
        datasrc_resource.property.extend(msg.property)
        datasrc_resource.station_id.extend(msg.station_id)

        datasrc_resource.source_type                   = msg.source_type
        datasrc_resource.request_type                  = msg.request_type
        datasrc_resource.request_bounds_north          = msg.request_bounds_north
        datasrc_resource.request_bounds_south          = msg.request_bounds_south
        datasrc_resource.request_bounds_west           = msg.request_bounds_west
        datasrc_resource.request_bounds_east           = msg.request_bounds_east
        datasrc_resource.base_url                      = msg.base_url
        datasrc_resource.dataset_url                   = msg.dataset_url
        datasrc_resource.ncml_mask                     = msg.ncml_mask
        datasrc_resource.max_ingest_millis             = msg.max_ingest_millis
        datasrc_resource.ion_title                     = msg.ion_title
        datasrc_resource.ion_institution_id            = msg.ion_institution_id
        datasrc_resource.update_start_datetime_millis  = msg.start_time


        #fixme, put it with the others
        yield self.rc.put_instance(datasrc_resource)
        log.info("created data source " + str(datasrc_resrource))

        defer.returnValue(datasrc_resource)
