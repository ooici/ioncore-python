#!/usr/bin/env python

"""
@file ion/integration/ais/manageDataResource/manageDataResource.py
@author Ian Katz
@brief The worker class that implements the data resource functions for the AIS  (workflow #105, #106)
"""
from ion.core.object.workbench import IDREF_TYPE
from ion.services.coi.datastore import DataStoreClient
from ion.services.dm.inventory.association_service import ASSOCIATION_GET_STAR_MSG_TYPE, PREDICATE_REFERENCE_TYPE, AssociationServiceClient, PREDICATE_OBJECT_QUERY_TYPE

import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.exception import ReceivedApplicationError, ReceivedContainerError
from ion.services.coi.resource_registry.resource_client import ResourceClient

from ion.core.object import object_utils

from ion.services.dm.inventory.dataset_controller import DatasetControllerClient
from ion.services.dm.ingestion.ingestion import IngestionClient
from ion.services.dm.scheduler.scheduler_service import SchedulerServiceClient, \
                                                        SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE

from ion.services.dm.distribution.events import ScheduleEventPublisher, \
                                                DatasourceChangeEventPublisher

from ion.util.iontime import IonTime
import time

from ion.core.process.process import Process



from ion.util.url import urlRe

from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient                                                         

from ion.services.coi.resource_registry.association_client import AssociationClient
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID, \
                                                                    DATASOURCE_RESOURCE_TYPE_ID, \
                                                                    DATARESOURCE_SCHEDULE_TYPE_ID, TYPE_OF_ID, HAS_LIFE_CYCLE_STATE_ID

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       CREATE_DATA_RESOURCE_REQ_TYPE, \
                                                       CREATE_DATA_RESOURCE_RSP_TYPE, \
                                                       UPDATE_DATA_RESOURCE_REQ_TYPE, \
                                                       UPDATE_DATA_RESOURCE_RSP_TYPE, \
                                                       DELETE_DATA_RESOURCE_REQ_TYPE, \
                                                       DELETE_DATA_RESOURCE_RSP_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_REQ_TYPE, \
                                                       DATA_RESOURCE_SCHEDULED_TASK_TYPE

INGESTER_CREATETOPICS_REQ_MSG  = object_utils.create_type_identifier(object_id=2003, version=1)
RESOURCE_CFG_REQUEST_TYPE      = object_utils.create_type_identifier(object_id=10, version=1)
RESOURCE_CFG_RESPONSE_TYPE     = object_utils.create_type_identifier(object_id=12, version=1)
SA_DATASOURCE_RESOURCE_MSG     = object_utils.create_type_identifier(object_id=4503, version=1)
SCHEDULER_ADD_REQ_TYPE         = object_utils.create_type_identifier(object_id=2601, version=1)
SCHEDULER_ADD_RSP_TYPE         = object_utils.create_type_identifier(object_id=2602, version=1)
SCHEDULER_DEL_REQ_TYPE         = object_utils.create_type_identifier(object_id=2603, version=1)
SCHEDULER_DEL_RSP_TYPE         = object_utils.create_type_identifier(object_id=2604, version=1)
SCHEDULER_PERFORM_INGEST       = object_utils.create_type_identifier(object_id=2607, version=1)
LCS_REFERENCE_TYPE             = object_utils.create_type_identifier(object_id=26, version=1)
GET_LCS_REQUEST_MESSAGE_TYPE   = object_utils.create_type_identifier(object_id=58, version=1)


DEFAULT_MAX_INGEST_MILLIS = 30000


class ManageDataResource(object):

    def __init__(self, ais):

        
        log.debug('ManageDataResource.__init__()')
        self._proc = ais
        self.mc    = ais.mc
        self.rc    = ais.rc
        self.dsc   = DataStoreClient(proc=ais)
        self.dscc  = DatasetControllerClient(proc=ais)
        self.ac    = AssociationClient(proc=ais)
        self.asc   = AssociationServiceClient(proc=ais)
        self.sc    = SchedulerServiceClient(proc=ais)

        # Not needed...
        #self.ing   = IngestionClient(proc=ais)
        self.nac   = NotificationAlertServiceClient(proc=ais)
         
        #necessary to receive events i think
        self.pub_schd   = ScheduleEventPublisher(process=ais)
        self.pub_dsrc   = DatasourceChangeEventPublisher(process=ais)

    @defer.inlineCallbacks
    def update(self, msg_wrapped):
        """
        @brief update a data resource
        @param msg GPB, 9215/1,
        @GPB{Input,9215,1}
        @GPB{Returns,9216,1}
        @retval success
        """
        log.debug('update worker class method')

        # check that the GPB is correct type & has a payload
        result = yield self._CheckRequest(msg_wrapped)
        if result != None:
            result.error_str = "AIS.ManageDataResource.update: " + result.error_str
            defer.returnValue(result)

        msg = msg_wrapped.message_parameters_reference 
        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if not self._equalInputTypes(msg_wrapped, msg, UPDATE_DATA_RESOURCE_REQ_TYPE):
                errtext = "AIS.ManageDataResource.update: " + \
                    "Expected DataResourceUpdateRequest type, got " + str(msg)
                log.error(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            if not (msg.IsFieldSet("data_set_resource_id")):
                errtext = "AIS.ManageDataResource.update: " + \
                    "required fields not provided (data_set_resource_id)"
                log.error(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            if msg.IsFieldSet("visualization_url") and msg.visualization_url != '':
                visualization_url = msg.visualization_url
                if (urlRe.match(visualization_url) is None):
                    errtext = "ManageDataResource.update(): " + \
                        "Visualization URL unexpected/invalid format: " + \
                        msg.visualization_url
                    log.error(errtext)
                    Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                    Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                    Response.error_str =  errtext
                    defer.returnValue(Response)

            #OOIION-164
            dateproblem = yield self._checkStartDatetime("ManageDataResource.update()", msg)
            if not dateproblem is None:
                defer.returnValue(dateproblem)
                

            dataset_resource = yield self.rc.get_instance(msg.data_set_resource_id)
            datasrc_resource = yield self._getOneAssociationSubject(dataset_resource, 
                                                                    HAS_A_ID, 
                                                                    DATASOURCE_RESOURCE_TYPE_ID)

            #watch for data inconsistency
            if datasrc_resource is None:
                errtext = "ManageDataResource.update(): " + \
                    "could not find a data source resource associated " + \
                    "with the data set at " + msg.data_set_resource_id
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

                Response.error_num =  Response.ResponseCodes.NOT_FOUND
                Response.error_str =  errtext
                defer.returnValue(Response)
                
            
            if msg.IsFieldSet("update_interval_seconds"):

                #if we are rescheduling or turning off updates, delete scheduled events
                if 0 >= msg.update_interval_seconds or msg.IsFieldSet("update_start_datetime_millis"):
                    yield self._deleteAllScheduledEvents(datasrc_resource)
                    datasrc_resource.update_interval_seconds = 0
                    datasrc_resource.update_start_datetime_millis = 0

                # if update_interval is sane, create the new schedule
                if 0 < msg.update_interval_seconds and msg.IsFieldSet("update_start_datetime_millis"):
                    datasrc_resource.update_interval_seconds = msg.update_interval_seconds
                    datasrc_resource.update_start_datetime_millis = msg.update_start_datetime_millis

                    #get the things we need to set up the scheduler message
                    log.info("Looking up data set resource")
                    dataset_resource = yield self._getOneAssociationObject(datasrc_resource, 
                                                                           HAS_A_ID, 
                                                                           DATASET_RESOURCE_TYPE_ID)

                    # @TODO Must update the existing scheduled event if it exists!
                    # May have to delete and re add?

                    #dataset_id = dataset_resource.ResourceIdentity
                    sched_task = yield self._createScheduledEvent(msg.update_interval_seconds,
                                                                  msg.update_start_datetime_millis,
                                                                  dataset_resource.ResourceIdentity,
                                                                  datasrc_resource.ResourceIdentity)

                    #log.info("got this from scheduler: " + str(sched_task))

                    log.info("associating scheduler task info with this resource")
                    sched_task_rsrc = yield self.rc.create_instance(DATA_RESOURCE_SCHEDULED_TASK_TYPE,
                                                                    ResourceName="ScheduledTask resource")

                    sched_task_rsrc.task_id = sched_task.task_id
                    yield self.ac.create_association(datasrc_resource, HAS_A_ID, sched_task_rsrc)
                    sched_task_rsrc.ResourceLifeCycleState  = sched_task_rsrc.ACTIVE
                    yield self.rc.put_instance(sched_task_rsrc)
                    
            if msg.IsFieldSet("ion_title"):
                datasrc_resource.ion_title = msg.ion_title

            if msg.IsFieldSet("ion_description"):
                datasrc_resource.ion_description = msg.ion_description

            if msg.IsFieldSet("max_ingest_millis"):
                datasrc_resource.max_ingest_millis = msg.max_ingest_millis

            if msg.IsFieldSet("is_public"):
                datasrc_resource.is_public = msg.is_public

            datasrc_resource.ResourceLifeCycleState = datasrc_resource.ACTIVE

            if msg.IsFieldSet("visualization_url") and msg.visualization_url != '':
                datasrc_resource.visualization_url = msg.visualization_url

            # This could be cleaned up to go faster - only call put if it is modified!
            yield self.rc.put_resource_transaction([datasrc_resource, dataset_resource])

            yield self.pub_dsrc.create_and_publish_event(origin=datasrc_resource.ResourceIdentity,
                                                         datasource_id=datasrc_resource.ResourceIdentity)


        except ReceivedApplicationError, ex:
            log.info('AIS.ManageDataResource.update: Error: %s' %ex)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  "AIS.ManageDataResource.update: Error from lower-level service: " + \
                ex.msg_content.MessageResponseBody
            defer.returnValue(Response)

        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        Response.result = 200
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(UPDATE_DATA_RESOURCE_RSP_TYPE)
        Response.message_parameters_reference[0].success = True
        defer.returnValue(Response)


    @defer.inlineCallbacks
    def delete(self, msg_wrapped):
        """
        @brief delete a data resource
        @param msg GPB, 9213/1,
        @GPB{Input,9213,1}
        @GPB{Returns,9214,1}
        @retval success
        """
        # check that the GPB is correct type & has a payload
        result = yield self._CheckRequest(msg_wrapped)
        if result != None:
            result.error_str = "AIS.ManageDataResource.delete: " + result.error_str
            defer.returnValue(result)

        msg = msg_wrapped.message_parameters_reference 
        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if not self._equalInputTypes(msg_wrapped, msg, DELETE_DATA_RESOURCE_REQ_TYPE):
                errtext = "AIS.ManageDataResource.delete: " + \
                    "Expected DataResourceDeleteRequest type, got " + str(msg)
                log.error(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            if not (msg.IsFieldSet("data_set_resource_id")):
                errtext = "AIS.ManageDataResource.delete: " + \
                    "required fields not provided (data_set_resource_id)"
                log.error(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            #store ids that were deleted, and return them later.
            deletions = []
            delete_resources = []
            for data_set_resource_id in msg.data_set_resource_id:

                #FIXME: if user does not own this data set, don't delete it

                log.info("Getting instance of data set resource with id = %s" % data_set_resource_id)
                dataset_resource = yield self.rc.get_instance(data_set_resource_id)
                log.info("Getting instance of datasource resource from association")
                datasrc_resource = yield self._getOneAssociationSubject(dataset_resource, 
                                                                       HAS_A_ID, 
                                                                       DATASOURCE_RESOURCE_TYPE_ID)

                log.info("got data src resource with id: %s" % datasrc_resource.ResourceIdentity)

                log.info("Deleting any attached scheduled ingest events")
                yield self._deleteAllScheduledEvents(datasrc_resource)

                log.info("Setting data source resource lifecycle = retired")
                datasrc_resource.ResourceLifeCycleState = datasrc_resource.RETIRED
                delete_resources.append(datasrc_resource)

                """
                ### Don't make changes to the data set resource - you AIS doesn't own it...
                if not None is dataset_resource:
                    log.info("Setting data set resource lifecycle = retired")
                    dataset_resource.ResourceLifeCycleState = dataset_resource.RETIRED
                    delete_resources.append(dataset_resource)
                """
                deletions.append(datasrc_resource.ResourceIdentity)


            log.info("putting all resource changes in one big transaction, " \
                         + str(len(delete_resources)))
            yield self.rc.put_resource_transaction(delete_resources)
            log.info("Success!")

            for datasrc_resource in delete_resources:
                log.info("creating event to signal caching service")
                yield self.pub_dsrc.create_and_publish_event(origin=datasrc_resource.ResourceIdentity,
                                                         datasource_id=datasrc_resource.ResourceIdentity)
            
        except ReceivedApplicationError, ex:
            log.info('AIS.ManageDataResource.delete: Error: %s' %ex)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  "AIS.ManageDataResource.delete: Error from lower-level service: " + \
                ex.msg_content.MessageResponseBody
            defer.returnValue(Response)

        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        Response.result = 200
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(DELETE_DATA_RESOURCE_RSP_TYPE)

        Response.message_parameters_reference[0].successfully_deleted_id.extend(deletions)

        defer.returnValue(Response)

    
    @defer.inlineCallbacks
    def create(self, msg_wrapped):
        """
        @brief create a data resource.  URL IS ASSUMED TO BE VALID NETCDF
        @param msg GPB, 9211/1,
        @GPB{Input,9211,1}
        @GPB{Returns,9212,1}
        @retval IDs of new objects, GPB 9212/1, otherwise an AIS error GPB
        """
        log.info('ManageDataResource.create()\n')

        my_datasrc_id      = None
        my_dataset_id      = None

        datasrc_resource      = None
        dataset_resource      = None

        # check that the GPB is correct type & has a payload
        result = yield self._CheckRequest(msg_wrapped)
        if result != None:
            result.error_str = "AIS.ManageDataResource.create: " + result.error_str
            defer.returnValue(result)

        msg = msg_wrapped.message_parameters_reference 
        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if not self._equalInputTypes(msg_wrapped, msg, CREATE_DATA_RESOURCE_REQ_TYPE):
                errtext = "AIS.ManageDataResource.create: " + \
                    "Expected DataResourceCreateRequest type, got " + str(msg)
                log.error(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            # make user at least PRETEND to make an effort in filling out fields
            missing = self._missingResourceRequestFields(msg)
            # unless nothing is missing, error.
            if "" != missing:
                errtext = "AIS.ManageDataResource.create: " + \
                    "Missing/incorrect required fields in DataResourceCreateRequest: " + missing
                log.error(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            #max_ingest_millis: default to 30000 (30 seconds before ingest timeout)
            #FIXME: find out what that default should really be.
            if not msg.IsFieldSet("max_ingest_millis") or msg.max_ingest_millis <= 0:
                if msg.IsFieldSet("update_interval_seconds") and msg.update_interval_seconds > 1:
                    msg.max_ingest_millis = (msg.update_interval_seconds - 1) * 1000
                else:
                    msg.max_ingest_millis = DEFAULT_MAX_INGEST_MILLIS

            #OOIION-164
            dateproblem = yield self._checkStartDatetime("ManageDataResource.create()", msg)
            if not dateproblem is None:
                defer.returnValue(dateproblem)

            if msg.IsFieldSet("visualization_url") and msg.visualization_url != '':
                visualization_url = msg.visualization_url
                if (urlRe.match(visualization_url) is None):
                    errtext = "ManageDataResource.create(): " + \
                        "Visualization URL unexpected/invalid format: " + \
                        msg.visualization_url
                    log.error(errtext)
                    Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                    Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                    Response.error_str =  errtext
                    defer.returnValue(Response)

            # create the data source from the fields in the input message
            datasrc_resource = yield self._createDataSourceResource(msg)
            my_datasrc_id = datasrc_resource.ResourceIdentity

            # create the dataset by sending a blank message to the dscc
            log.info("manage_data_resource calling create dataset")
            dataset_req = yield self.mc.create_instance(None)
            # we get back the ID, so look up resource from that
            tmp = yield self.dscc.create_dataset_resource(dataset_req)
            my_dataset_id = tmp.key
            dataset_resource = yield self.rc.get_instance(my_dataset_id)
            log.info("created data set ") # + str(dataset_resource))

            # create topics
            """
            ### Do not do this - it is not needed in the R1 Deployment configuration
            topics_msg = yield self.mc.create_instance(INGESTER_CREATETOPICS_REQ_MSG)
            topics_msg.dataset_id = my_dataset_id
            #don't yield on this. - It may be that you don't need to yield here because you don't need the result but
            # it is exetremely dangerous to do this in the R1 architecture!
            self.ing.create_dataset_topics(topics_msg)
            """
            #make associations
            association_d = yield self.ac.create_association(datasrc_resource, HAS_A_ID, dataset_resource)

            #build transaction
            resource_transaction = [datasrc_resource, dataset_resource]

            if (msg.IsFieldSet("update_interval_seconds") and \
                    msg.IsFieldSet("update_start_datetime_millis") and \
                    msg.update_interval_seconds > 0):

                #record values
                datasrc_resource.update_interval_seconds       = msg.update_interval_seconds
                datasrc_resource.update_start_datetime_millis  = msg.update_start_datetime_millis

                # set up the scheduled task
                sched_task = yield self._createScheduledEvent(msg.update_interval_seconds,
                                                              msg.update_start_datetime_millis,
                                                              my_dataset_id,
                                                              my_datasrc_id)
                #log.info("got this from scheduler: " + str(sched_task))

                sched_task_rsrc = yield self.rc.create_instance(DATA_RESOURCE_SCHEDULED_TASK_TYPE,
                                                                ResourceName="ScheduledTask resource")

                sched_task_rsrc.task_id = sched_task.task_id
                yield self.ac.create_association(datasrc_resource, HAS_A_ID, sched_task_rsrc)
                sched_task_rsrc.ResourceLifeCycleState  = sched_task_rsrc.ACTIVE
                resource_transaction.append(sched_task_rsrc)

            #resource lifecycle states as per 6/27/11 call with dstuebe
            datasrc_resource.ResourceLifeCycleState = datasrc_resource.ACTIVE
            dataset_resource.ResourceLifeCycleState = dataset_resource.NEW

            # create subscription to catch the ingestion-complete or failure events
            yield self._createSubscription(my_datasrc_id, msg.user_id, datasrc_resource.ion_title)
            
            yield self.rc.put_resource_transaction(resource_transaction)

            yield self._createEvent(my_dataset_id, my_datasrc_id)

        except ReceivedApplicationError, ex:
            log.info('AIS.ManageDataResource.create: Error from a lower-level service: %s' %ex)

            #mark lifecycle states
            datasrc_resource.ResourceLifeCycleState = datasrc_resource.RETIRED
            dataset_resource.ResourceLifeCycleState = dataset_resource.RETIRED
            sched_task_rsrc.ResourceLifeCycleState  = sched_task_rsrc.RETIRED
            yield self.rc.put_resource_transaction([datasrc_resource, dataset_resource, sched_task_rsrc])

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  "AIS.ManageDataResource.create: Error from lower-level service: " + \
                ex.msg_content.MessageResponseBody
            defer.returnValue(Response)

        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        Response.result = 200
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(CREATE_DATA_RESOURCE_RSP_TYPE)
        Response.message_parameters_reference[0].data_source_id  = my_datasrc_id
        Response.message_parameters_reference[0].data_set_id     = my_dataset_id
        Response.message_parameters_reference[0].association_id  = association_d.AssociationIdentity
        defer.returnValue(Response)


    @defer.inlineCallbacks
    def _createSubscription(self, datasrc_id, user_id, ion_title):
        log.info("AIS.ManageDataResource._createSubscription: ")
        #
        # Add a subscription for this user to this dataset resource
        #
        reqMsg = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = datasrc_id
        # set up subscription for email notification only
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAIL
        # set up subscription to catch both successful and failed ingestions
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATESANDDATASOURCEOFFLINE

        # set up the meta data so that the NAS is happy
        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = datasrc_id
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms
        reqMsg.message_parameters_reference.datasetMetadata.title = "Initial ingestion of " + ion_title;
        # indicate to the NAS that this is an automatically created email subscription that should be deleted after the initial
        # ingestion event is received
        reqMsg.message_parameters_reference.subscriptionInfo.dispatcher_script_path = "AutomaticallyCreatedInitialIngestionSubscription"
        
        try:
            log.debug("AIS.ManageDataResource._createSubscription: calling notification alert service addSubscription()")
            yield self.nac.addSubscription(reqMsg)
        except ReceivedApplicationError, ex:
            log.info('AIS.ManageDataResource._createSubscription(): Error attempting to addSubscription(): %s' %ex)
       
        
    @defer.inlineCallbacks
    def _createEvent(self, dataset_id, datasource_id):
        """
        @brief create a single ingest event trigger and send it
        """
        log.info("triggering an immediate ingest event")
        msg = yield self.pub_schd.create_event(origin=SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE,
                                               task_id="manage_data_resource_FAKED_TASK_ID")
        
        msg.additional_data.payload = msg.CreateObject(SCHEDULER_PERFORM_INGEST)
        msg.additional_data.payload.dataset_id     = dataset_id
        msg.additional_data.payload.datasource_id  = datasource_id

        yield self.pub_schd.publish_event(msg, origin=SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE)

        log.info("creating event to signal caching service")
        yield self.pub_dsrc.create_and_publish_event(origin=datasource_id, datasource_id=datasource_id)



    @defer.inlineCallbacks
    def _createScheduledEvent(self, interval, start_time, dataset_id, datasource_id):
        """
        @brief create a scheduled event and return its task_id
        """
        log.info("Creating scheduler task")

        log.info("Creating add-request for scheduler")
        req_msg = yield self.mc.create_instance(SCHEDULER_ADD_REQ_TYPE)
        req_msg.interval_seconds       = interval
        req_msg.start_time             = start_time
        req_msg.desired_origin = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE

        req_msg.payload                = req_msg.CreateObject(SCHEDULER_PERFORM_INGEST)
        req_msg.payload.dataset_id     = dataset_id
        req_msg.payload.datasource_id  = datasource_id

        log.info("sending request to scheduler")
        response = yield self.sc.add_task(req_msg)
        log.info("got response")
        defer.returnValue(response)


    @defer.inlineCallbacks
    def _deleteAllScheduledEvents(self, data_source_resource):
        """
        @brief delete any scheduled events from scheduler and storage
        """

        log.info("Getting instances of scheduled task resources associated with this data source: %s"
                 % data_source_resource.ResourceIdentity)

        sched_task_ids = yield self._getAllAssociationObjects(data_source_resource,
                                                              HAS_A_ID,
                                                              DATARESOURCE_SCHEDULE_TYPE_ID)
        for sched_task_id in sched_task_ids:
            sched_task_rsrc = yield self.rc.get_instance(sched_task_id)

            req_msg = yield self.mc.create_instance(SCHEDULER_DEL_REQ_TYPE)
            req_msg.task_id = sched_task_rsrc.task_id
            response = yield self.sc.rm_task(req_msg)
            log.info("Scheduler rm_task response %s" % str(response.MessageResponseCode))

            sched_task_rsrc.ResourceLifeCycleState = sched_task_rsrc.RETIRED
            yield self.rc.put_instance(sched_task_rsrc)

        defer.returnValue(None)

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
        datasrc_resource.is_public                     = msg.is_public
        datasrc_resource.visualization_url             = msg.visualization_url
        datasrc_resource.timestep_file_count           = msg.timestep_file_count

        # from bug OOIION-131
        datasrc_resource.initial_starttime_offset_millis  = msg.initial_starttime_offset_millis
        datasrc_resource.starttime_offset_millis          = msg.starttime_offset_millis
        datasrc_resource.endtime_offset_millis            = msg.endtime_offset_millis
        datasrc_resource.aggregation_rule                 = msg.aggregation_rule

        for i, r in enumerate(msg.sub_ranges):
            s = datasrc_resource.sub_ranges.add()
            s.dim_name    = r.dim_name
            s.start_index = r.start_index
            s.end_index   = r.end_index

        if msg.IsFieldSet('authentication'):
            log.info("Setting datasource: authentication")
            datasrc_resource.authentication                = msg.authentication

        if msg.IsFieldSet('search_pattern'):
            log.info("Setting datasource: search_pattern")
            datasrc_resource.search_pattern                = msg.search_pattern

        datasrc_resource.registration_datetime_millis  = IonTime().time_ms

        # Set the lifecycle state to active
        datasrc_resource.ResourceLifeCycleState        = datasrc_resource.ACTIVE

        #Do not put the resource at this time!
        #yield self.rc.put_instance(datasrc_resource)
        log.info("created data source ") # + str(datasrc_resource))

        defer.returnValue(datasrc_resource)

    @defer.inlineCallbacks
    def _getAllAssociationObjects(self, the_subject, the_predicate, the_object_type):
        """
        Get a list of all resource key ids that are the object side of an association to the passed in subject.
        The resource's LCS is checked to make sure it is not RETIRED.

        @return A list of resource id strings, or an empty list.
        """
        request = yield self._proc.message_client.create_instance(ASSOCIATION_GET_STAR_MSG_TYPE)

        pair = request.subject_pairs.add()
        pair.subject = request.CreateObject(IDREF_TYPE)
        pair.subject.key = the_subject.ResourceIdentity

        pair.predicate = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pair.predicate.key = the_predicate

        # first right side: TYPEOF == the_object_type
        pair = request.object_pairs.add()
        pair.object = request.CreateObject(IDREF_TYPE)
        pair.object.key = the_object_type

        pair.predicate = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pair.predicate.key = TYPE_OF_ID

        results = yield self.asc.get_star(request)
        stars = [str(x.key) for x in results.idrefs]

        log.debug("stars is %s" % str(stars))

        if len(stars) > 1:
            log.info("_getAllAssociationObjects found %d objects prior to LCS check" % len(stars))
        elif len(stars) == 0:
            log.info("_getAllAssociationObjects: NO association found")
            defer.returnValue([])

        # unfortunatly, we have no way to query for something != something, so we have to check LCS manually
        # we do this more optimizedly by querying the DS for the most recent commit
        request = yield self._proc.message_client.create_instance(GET_LCS_REQUEST_MESSAGE_TYPE)
        request.keys.extend(stars)

        lcses = yield self.dsc.get_lcs(request)

        # filter them for RETIRED
        res_keys = [x.key for x in filter(lambda pair: pair.lcs != pair.LifeCycleState.RETIRED, lcses.key_lcs_pairs)]
        log.debug("_getAllAssociationObjects: returning keys %s" % str(res_keys))
        defer.returnValue(res_keys)

    @defer.inlineCallbacks
    def _getOneAssociationObject(self, the_subject, the_predicate, the_object_type):
        """
        @brief get the object side of an association when you only expect one
        @return id of what you're after
        """
        res_keys = yield self._getAllAssociationObjects(the_subject, the_predicate, the_object_type)
        if len(res_keys) == 0:
            log.info("_getOneAssociationObject: no matching objects with LCS != RETIRED found, returning None")
            defer.returnValue(None)

        resource = yield self.rc.get_instance(res_keys[0])
        defer.returnValue(resource)

    @defer.inlineCallbacks
    def _getAllAssociationSubjects(self, the_object, the_predicate, the_subject_type):
        """
        Get a list of all resource key ids that are the subject side of an association to the passed in object.
        The resource's LCS is checked to make sure it is not RETIRED.

        @return A list of resource id strings, or an empty list.
        """
        request = yield self._proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
        pair = request.pairs.add()
        pair.object = request.CreateObject(IDREF_TYPE)
        pair.object.key = the_object.ResourceIdentity

        pair.predicate = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pair.predicate.key = the_predicate

        pair = request.pairs.add()
        pair.object = request.CreateObject(IDREF_TYPE)
        pair.object.key = the_subject_type

        pair.predicate = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pair.predicate.key = TYPE_OF_ID

        results = yield self.asc.get_subjects(request)
        subjs = [str(x.key) for x in results.idrefs]

        #log.debug("assocs is %s" % str(subjs))

        if len(subjs) > 1:
            log.info("_getAllAssociationSubjects: found %d subjects prior to LCS check" % len(stars))
        elif len(subjs) == 0:
            log.info("_getAllAssociationSubjects: NO association found")
            defer.returnValue([])

        # unfortunatly, we have no way to query for something != something, so we have to check LCS manually
        # we do this more optimizedly by querying the DS for the most recent commit
        request = yield self._proc.message_client.create_instance(GET_LCS_REQUEST_MESSAGE_TYPE)
        request.keys.extend(subjs)

        lcses = yield self.dsc.get_lcs(request)

        # filter them for RETIRED
        res_keys = [x.key for x in filter(lambda pair: pair.lcs != pair.LifeCycleState.RETIRED, lcses.key_lcs_pairs)]
        log.debug("_getAllAssociationSubjects: returning keys %s" % str(res_keys))
        defer.returnValue(res_keys)

    @defer.inlineCallbacks
    def _getOneAssociationSubject(self, the_object, the_predicate, the_subject_type):
        """
        @brief get the subject side of an association when you only expect one
        @return A resource, or None
        """
        res_keys = yield self._getAllAssociationSubjects(the_object, the_predicate, the_subject_type)
        if len(res_keys) == 0:
            log.info("_getOneAssociationSubject: no matching subjects with LCS != RETIRED found, returning None")
            defer.returnValue(None)

        resource = yield self.rc.get_instance(res_keys[0])
        defer.returnValue(resource)

    def _missingResourceRequestFields(self, msg):
        """
        @brief make sure that all the required fields are set
        @return string empty string for no errors or message describing the unset fields
        """

        #this seems very un-GPB-ish, to have to check fields...
        req_fields = [#"user_id", #deprecated as per OOIION-240
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
                      #"update_interval_seconds",
                      #"update_start_datetime_millis",
                      "is_public",
                      ]

        #these repeated fields don't need to be set either
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


    def _equalInputTypes(self, ais_req_msg, some_casref, desired_type):
        test_msg = ais_req_msg.CreateObject(desired_type)
        return (type(test_msg) == type(some_casref))
        

    @defer.inlineCallbacks
    def _CheckRequest(self, request):
       # Check for correct request protocol buffer type
       if request.MessageType != AIS_REQUEST_MSG_TYPE:
          # build AIS error response
          Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
          Response.error_num = Response.ResponseCodes.BAD_REQUEST
          Response.error_str = 'Bad message type receieved, ignoring'
          defer.returnValue(Response)
 
       # Check payload in message
       if not request.IsFieldSet('message_parameters_reference'):
          # build AIS error response
          Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
          Response.error_num = Response.ResponseCodes.BAD_REQUEST
          Response.error_str = "Required field [message_parameters_reference] not found in message"
          defer.returnValue(Response)
   
       defer.returnValue(None)


    #OOIION-164: check that the start date is less than a year from now
    @defer.inlineCallbacks
    def _checkStartDatetime(self, caller, msg):
        if msg.IsFieldSet("update_start_datetime_millis") \
                and msg.update_start_datetime_millis > ((time.time() + 31536000) * 1000):
            errtext = caller + ": Got a start date in milliseconds that was more than 1 year in the future ("
            errtext = errtext + str(msg.update_start_datetime_millis) + ")."  
            log.error(errtext)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
            Response.error_num =  Response.ResponseCodes.BAD_REQUEST
            Response.error_str =  errtext
            defer.returnValue(Response)

        defer.returnValue(None)
