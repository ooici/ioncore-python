#!/usr/bin/env python

"""
@file ion/integration/ais/manageDataResource/manageDataResource.py
@author Ian Katz
@brief The worker class that implements the data resource functions for the AIS  (workflow #105, #106)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.messaging.message_client import MessageClient
from ion.services.dm.inventory.dataset_controller import DatasetControllerClient
from ion.services.dm.ingestion.ingestion import IngestionClient

from ion.core.exception import ReceivedApplicationError, ReceivedContainerError

from ion.services.coi.resource_registry.association_client import AssociationClient

from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, \
                                                                    TYPE_OF_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID, \
                                                                    DATASOURCE_RESOURCE_TYPE_ID

from ion.services.coi.resource_registry.resource_client import ResourceClient, \
                                                                    ResourceInstance
from ion.services.coi.resource_registry.resource_client import ResourceClientError, \
                                                                    ResourceInstanceError

from ion.core.object import object_utils

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       CREATE_DATA_RESOURCE_REQ_TYPE, \
                                                       CREATE_DATA_RESOURCE_RSP_TYPE, \
                                                       UPDATE_DATA_RESOURCE_REQ_TYPE, \
                                                       UPDATE_DATA_RESOURCE_RSP_TYPE, \
                                                       DELETE_DATA_RESOURCE_REQ_TYPE, \
                                                       DELETE_DATA_RESOURCE_RSP_TYPE

INGESTER_CREATETOPICS_REQ_MSG  = object_utils.create_type_identifier(object_id=2003, version=1)
RESOURCE_CFG_REQUEST_TYPE      = object_utils.create_type_identifier(object_id=10, version=1)
RESOURCE_CFG_RESPONSE_TYPE     = object_utils.create_type_identifier(object_id=12, version=1)
SA_DATASOURCE_RESOURCE_MSG     = object_utils.create_type_identifier(object_id=4503, version=1)
SCHEDULER_ADD_REQ_TYPE         = object_utils.create_type_identifier(object_id=2601, version=1)
SCHEDULER_ADD_RSP_TYPE         = object_utils.create_type_identifier(object_id=2602, version=1)


DEFAULT_MAX_INGEST_MILLIS = 30000


class ManageDataResource(object):

    def __init__(self, ais):
        log.debug('ManageDataResource.__init__()')
        self.mc    = ais.mc
        self.rc    = ais.rc
        self.dscc  = DatasetControllerClient(proc=ais)
        self.ac    = AssociationClient(proc=ais)
        self.ing   = IngestionClient(proc=ais)


    @defer.inlineCallbacks
    def update(self, msg):
        """
        @brief update a data resource
        @param msg GPB, 9215/1,
        @GPB{Input,9215,1}
        @GPB{Returns,9216,1}
        @retval success
        """
        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if msg.MessageType != UPDATE_DATA_RESOURCE_REQ_TYPE:
                errtext = "ManageDataResource.update(): " + \
                    "Expected DataResourceUpdateRequest type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            if not (msg.IsFieldSet("data_source_resource_id")):

                errtext = "ManageDataResource.update(): " + \
                    "required fields not provided (data_source_resource_id)"
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            datasrc_resource  = yield self.rc.get_instance(msg.data_source_resource_id)

            if msg.IsFieldSet("update_interval_seconds"):
                dispatcher_resource.update_interval_seconds = msg.update_interval_seconds
                #FIXME: change scheduling

            if msg.IsFieldSet("ion_institution_id"):
                dispatcher_resource.ion_institution_id = msg.ion_institution_id

            if msg.IsFieldSet("ion_description"):
                dispatcher_resource.ion_description = msg.ion_description

            yield self.rc.put_resource(datasrc_resource)


        except ReceivedApplicationError, ex:
            log.info('ManageDataResource.update(): Error: %s' %ex)

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)



        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)

        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(UPDATE_DATA_RESOURCE_RSP_TYPE)
        Response.message_parameters_reference[0].success = True
        defer.returnValue(Response)



    @defer.inlineCallbacks
    def delete(self, msg):
        """
        @brief delete a data resource
        @param msg GPB, 9213/1,
        @GPB{Input,9213,1}
        @GPB{Returns,9214,1}
        @retval success
        """
        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if msg.MessageType != DELETE_DATA_RESOURCE_REQ_TYPE:
                errtext = "ManageDataResource.delete(): " + \
                    "Expected DataResourceDeleteRequest type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            if not (msg.IsFieldSet("data_source_resource_id")):

                errtext = "ManageDataResource.delete(): " + \
                    "required fields not provided (data_source_resource_id)"
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            #store ids that were deleted, and return them later.
            deletions = []
            delete_resources = []
            for data_source_resource_id in msg.data_source_resource_id:

                #FIXME: if user does not own this data set, don't delete it

                #FIXME: stop scheduling
                log.info("Getting instance of data source resource")
                datasrc_resource = yield self.rc.get_instance(data_source_resource_id)
                log.info("Getting instance of dataset resource from association")
                dataset_resource = yield self._getOneAssociationObject(datasrc_resource, HAS_A_ID)

                log.info("Setting data source resource lifecycle = retired")
                datasrc_resource.ResourceLifeCycleState = datasrc_resource.RETIRED
                delete_resources.append(datasrc_resource)

                if not None is dataset_resource:
                    log.info("Setting data set resource lifecycle = retired")
                    dataset_resource.ResourceLifeCycleState = dataset_resource.RETIRED
                    delete_resources.append(dataset_resource)


                deletions.append(data_source_resource_id)

            log.info("putting all resource changes in one big transaction")
            yield self.rc.put_resource_transaction(delete_resources)



        except ReceivedApplicationError, ex:
            log.info('ManageDataResource.delete(): Error: %s' %ex)

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)


        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)

        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(UPDATE_DATA_RESOURCE_RSP_TYPE)
        for d in deletions:
            i = len(Response.message_parameters_reference[0].successfully_deleted_id)
            Response.message_parameters_reference[0].successfully_deleted_id.add()
            Response.message_parameters_reference[0].successfully_deleted_id[i] = d

        defer.returnValue(Response)





    @defer.inlineCallbacks
    def create(self, msg):
        """
        @brief create a data resource
        @param msg GPB, 9211/1,
        @GPB{Input,9211,1}
        @GPB{Returns,9212,1}
        @retval IDs of new objects, GPB 9212/1, otherwise an AIS error GPB
        """
        log.info('ManageDataResource.create()\n')

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
                errtext = "ManageDataResource.create(): " + \
                    "Expected DataResourceCreateRequest type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            # make user at least PRETEND to make an effort in filling out fields
            missing = self._missingResourceRequestFields(msg)
            # unless nothing is missing, error.
            if "" != missing:
                errtext = "ManageDataResource.create(): " + \
                    "Missing/incorrect required fields in DataResourceCreateRequest: " + missing
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)


            #FIXME: need to do cfchecker validation before we proceed.  this service doesn't exist yet.

            #max_ingest_millis: default to 30000 (30 seconds before ingest timeout)
            #FIXME: find out what that default should really be.  
            if not msg.IsFieldSet("max_ingest_millis"):
                msg.max_ingest_millis = DEFAULT_MAX_INGEST_MILLIS


            # get user resource so we can associate it later
            user_resource = yield self.rc.get_instance(msg.user_id)

            # create the data source
            datasrc_resource = yield self._createDataSourceResource(msg)
            my_datasrc_id = datasrc_resource.ResourceIdentity

            # create the dataset by sending a blank message to the dscc
            log.info("manage_data_resource calling create dataset")
            dataset_req = yield self.mc.create_instance(None)
            # we get back the ID, so look up resource from that
            tmp = yield self.dscc.create_dataset_resource(dataset_req)
            my_dataset_id = tmp.key
            dataset_resource = yield self.rc.get_instance(my_dataset_id)
            log.info("created data set " + str(dataset_resource))


            # create topics
            topics_msg = yield self.mc.create_instance(INGESTER_CREATETOPICS_REQ_MSG)
            topics_msg.dataset_id = my_dataset_id
            self.ing.create_dataset_topics(topics_msg)

            # FIXME call the scheduler service client
            #  it returns the scheduler task id, which i'll associate with the data source
            #
            # interval_seconds=uint64, origin=string, payload
            # response is UUID + origin
            #
            # payload is the UpdateEvent, which contains the dataset id
            # target is the DS update topic


            #make association
            #association = yield self.ac.create_association(user_resource,    HAS_A_ID, datasrc_resource)
            association = yield self.ac.create_association(datasrc_resource, HAS_A_ID, dataset_resource)


            #mark lifecycle states
            datasrc_resource.ResourceLifeCycleState = datasrc_resource.ACTIVE
            dataset_resource.ResourceLifeCycleState = dataset_resource.ACTIVE
            yield self.rc.put_resource_transaction([datasrc_resource, dataset_resource])


        except ReceivedApplicationError, ex:
            log.info('ManageDataResource.create(): Error attempting to FIXME: %s' %ex)

            #mark lifecycle states
            datasrc_resource.ResourceLifeCycleState = datasrc_resource.RETIRED
            dataset_resource.ResourceLifeCycleState = dataset_resource.RETIRED
            yield self.rc.put_resource_transaction([datasrc_resource, dataset_resource])

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)



        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)

        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(CREATE_DATA_RESOURCE_RSP_TYPE)
        Response.message_parameters_reference[0].data_source_id  = my_datasrc_id
        Response.message_parameters_reference[0].data_set_id     = my_dataset_id
        Response.message_parameters_reference[0].association_id  = association.AssociationIdentity
        defer.returnValue(Response)


    def _createScheduledEvent(self, desired_origin, interval_seconds, payload):
        sched_resource = yield self.mc.create_instance(SA_DATASOURCE_RESOURCE_MSG)

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
        log.info('ManageDataResource._createDataSourceResource()\n')
        datasrc_resource = yield self.rc.create_instance(SA_DATASOURCE_RESOURCE_MSG,
                                                         ResourceName='Data Source Resource')

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
        datasrc_resource.ion_description               = msg.ion_description
        datasrc_resource.ion_institution_id            = msg.ion_institution_id
        datasrc_resource.update_start_datetime_millis  = msg.update_start_datetime_millis


        #fixme, put it with the others
        yield self.rc.put_instance(datasrc_resource)
        log.info("created data source " + str(datasrc_resource))

        defer.returnValue(datasrc_resource)



    @defer.inlineCallbacks
    def _getOneAssociationObject(self, the_subject, the_predicate):
        """
        @brief get the subject side of an association when you only expect one
        @return id of what you're after
        """

        #can also do obj=
        found = yield self.ac.find_associations(subject=the_subject, \
                                                predicate_or_predicates=HAS_A_ID)

        association = None
        for a in found:
            exists = yield self.ac.association_exists(a.ObjectReference.key, TYPE_OF_ID, DATASOURCE_RESOURCE_TYPE_ID)
            if exists:
                #FIXME: if not association is None then we have data inconsistency!
                association = a

        #this is an error case!
        if None is association:
            defer.returnValue(None)


        the_resource = yield self.rc.get_associated_resource_object(association)
        defer.returnValue(the_resource)



    def _missingResourceRequestFields(self, msg):
        """
        @brief make sure that all the required fields are set
        @return string empty string for no errors or message describing the unset fields
        """

        #this seems very un-GPB-ish, to have to check fields...
        #FIXME: comment out things that aren't actually required
        req_fields = ["user_id", 
                      "source_type",
                      "request_type",
                      #"request_bounds_north",
                      #"request_bounds_south",
                      #"request_bounds_west",
                      #"request_bounds_east",
                      #"ncml_mask",
                      #"max_ingest_millis",
                      "ion_title",
                      "ion_description",
                      "ion_institution_id",
                      "update_interval_seconds",
                      "update_start_datetime_millis",
                      ]
                      
        
        #FIXME: what do about these repeated fields?
        #repeated string property = 3;
        #repeated string station_id = 4;

        #check em
        ret = ""

        if msg.IsFieldSet("base_url") and msg.IsFieldSet("dataset_url"):
            ret = "ONLY ONE OF (base_url, dataset_url)"
        elif not (msg.IsFieldSet("base_url") or msg.IsFieldSet("dataset_url")):
            ret = "ONE OF (base_url, dataset_url)"

        for f in req_fields:
            if not msg.IsFieldSet(f):
                if "" == ret:
                    ret = f
                else:
                    ret = ret + ", " + f

        return ret
