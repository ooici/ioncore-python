#!/usr/bin/env python

"""
@file ion/integration/ais/test/test_manage_data_resource.py
@test ion.integration.app_integration_service
@author Ian Katz
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

#from ion.core.object import object_utils

from ion.core.data.storage_configuration_utility import COMMIT_CACHE
#from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS
#from ion.services.coi.datastore_bootstrap.ion_preload_config import MYOOICI_USER_ID, ROOT_USER_ID, ANONYMOUS_USER_ID

import time

from ion.util import procutils as pu
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG, ION_AIS_RESOURCES_CFG

from ion.core.process.process import Process
from ion.test.iontest import IonTestCase

from ion.integration.ais.app_integration_service import AppIntegrationServiceClient

from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry.association_client import AssociationClient
from ion.services.coi.datastore_bootstrap.ion_preload_config import MYOOICI_USER_ID

from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID, \
                                                                    DATASOURCE_RESOURCE_TYPE_ID, \
                                                                    DATARESOURCE_SCHEDULE_TYPE_ID


from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       CREATE_DATA_RESOURCE_REQ_TYPE, \
                                                       UPDATE_DATA_RESOURCE_REQ_TYPE, \
                                                       DELETE_DATA_RESOURCE_REQ_TYPE



from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_PROFILE_DATA_SOURCE_ID, \
                                                                    SAMPLE_PROFILE_DATASET_ID



#from ion.integration.ais.manage_data_resource.manage_data_resource import DEFAULT_MAX_INGEST_MILLIS


class AISManageDataResourceTest(IonTestCase):

    """
    Testing Application Integration Service.
    """

    timeout = 120

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        #store.Store.kvs.clear()
        #store.IndexStore.kvs.clear()
        #store.IndexStore.indices.clear()

        self.dispatcher_id = None

        services = [

            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                'spawnargs':
                    {
                        PRELOAD_CFG:
                            {
                                ION_DATASETS_CFG:True,
                                ION_AIS_RESOURCES_CFG:True
                            },
                        COMMIT_CACHE:'ion.core.data.store.IndexStore'
                    }
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry.resource_registry',
                'class':'ResourceRegistryService',
                'spawnargs':
                    {
                        'datastore_service':'datastore'}
            },

            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },

            {
                'name':'identity_registry',
                'module':'ion.services.coi.identity_registry',
                'class':'IdentityRegistryService'
            },

            {
                'name':'dataset_controller',
                'module':'ion.services.dm.inventory.dataset_controller',
                'class':'DatasetControllerClient'
            },
            {
                'name':'scheduler_service',
                'module':'ion.services.dm.scheduler.scheduler_service',
                'class':'SchedulerServiceClient'
            },
            {
                'name':'app_integration',
                'module':'ion.integration.ais.app_integration_service',
                'class':'AppIntegrationService'
            },
            {
                'name':'notification_alert',
                'module':'ion.integration.ais.notification_alert_service',
                'class':'NotificationAlertService'
            },


            ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')

        self.sup = sup

        proc = Process()
        yield proc.spawn()

        self.aisc = AppIntegrationServiceClient(proc=proc)

        self.rc    = ResourceClient(proc=proc)
        self.mc    = MessageClient(proc=proc)
        self.ac    = AssociationClient(proc=proc)


        ### Test should now pass with the cache on!
        #prepare to monkey patch so we don't use the cache functions
        child_aiss = yield self.sup.get_child_id('app_integration')
        self.aiss  = self._get_procinstance(child_aiss)

        # For testing purposes - turn of the subscriber and remove the call back to update the cache - for now!
        self.aiss.dataset_subscriber.terminate()
        self.aiss.datasource_subscriber.terminate()

    @defer.inlineCallbacks
    def tearDown(self):
        log.info('Tearing Down Test Container')

        #store.Store.kvs.clear()
        #store.IndexStore.kvs.clear()
        #store.IndexStore.indices.clear()

        yield pu.asleep(1)
        yield self._shutdown_processes()
        yield self._stop_container()
        log.info("Successfully tore down test container")

    @defer.inlineCallbacks
    def test_instantiate(self):
        yield pu.asleep(1)

    @defer.inlineCallbacks
    def test_createDataResource(self):
        yield self._createDataResource()

    @defer.inlineCallbacks
    def test_createDeleteDataResource(self):
        #run the create
        create_resp = yield self._createDataResource()

        #try the delete
        yield self._deleteDataResource([create_resp.data_set_id])

    @defer.inlineCallbacks
    def test_createUpdateDeleteDataResource(self):

        #run the create
        log.info("FULL USAGE 1/3: create")
        create_resp = yield self._createDataResource()

        #run the update
        log.info("FULL USAGE 2/3: update")
        yield self._updateDataResource(create_resp.data_set_id)

        #try the delete
        log.info("FULL USAGE 3/3: delete")
        yield self._deleteDataResource([create_resp.data_set_id])
        log.info("Create/Update/Delete/COMPLETE")


    @defer.inlineCallbacks
    def test_reproduceOOIION451(self):

        #run the create
        log.info("creating 3 data sets (and using 2 existing data sets)")
        create_resp1 = yield self._createDataResource()
        create_resp2 = yield self._createDataResource()
        create_resp3 = yield self._createDataResource()
        create_resp4 = yield self._createDataResource()

        #try the delete
        log.info("deleting 2 data sets (new/existing)")
        yield self._deleteDataResource([create_resp1.data_set_id, create_resp4.data_set_id])

        log.info("deleting 2 data sets (existing/new)")
        yield self._deleteDataResource([SAMPLE_PROFILE_DATASET_ID, create_resp2.data_set_id])

        log.info("deleting last data set")
        yield self._deleteDataResource([create_resp3.data_set_id])
        

    @defer.inlineCallbacks
    def test_updateDataResource_BadInput(self):
        """
        run through the update code but don't specify any ids.
        """

        log.info("Trying to call updateDataResource with the wrong GPB")
        update_req_msg  = yield self.mc.create_instance(UPDATE_DATA_RESOURCE_REQ_TYPE)
        result       = yield self.aisc.updateDataResource(update_req_msg, MYOOICI_USER_ID)
        self.failUnlessEqual(result.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "updateDataResource accepted a GPB that was known to be the wrong type")


        log.info("Trying to call updateDataResource with an empty GPB")
        ais_req_msg  = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        update_req_msg  = ais_req_msg.CreateObject(UPDATE_DATA_RESOURCE_REQ_TYPE)
        ais_req_msg.message_parameters_reference = update_req_msg
        result_wrapped = yield self.aisc.updateDataResource(ais_req_msg, MYOOICI_USER_ID)
        self.failUnlessEqual(result_wrapped.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "updateDataResource accepted a GPB without a data_source_resource_id")


    @defer.inlineCallbacks
    def test_updateDataResourceSample(self):
        """
        try updating one of the preloaded data sources
        """

        log.info("Updating a sample data resource")
        yield self._updateDataResource(SAMPLE_PROFILE_DATASET_ID)

    @defer.inlineCallbacks
    def test_sampleDataAssociations(self):
        """
        see if the sample data sets are properly associated
        """

        dataset_resource = yield self.rc.get_instance(SAMPLE_PROFILE_DATASET_ID)
        self.failIfEqual(None, dataset_resource, "Data set resource doesn't appear to exist")

        datasrc_resource = yield self.rc.get_instance(SAMPLE_PROFILE_DATA_SOURCE_ID)
        self.failIfEqual(None, datasrc_resource, "Data source resource doesn't appear to exist")


        a_datasrc_resource = yield self._getOneAssociationSubject(dataset_resource,
                                                                  HAS_A_ID,
                                                                  DATASOURCE_RESOURCE_TYPE_ID)

        self.failIfEqual(None, a_datasrc_resource, "Data source resource doesn't appear to be associated with set")

        log.info("retrieved datasrc resource: %s" % a_datasrc_resource.ResourceIdentity)

    @defer.inlineCallbacks
    def _updateDataResource(self, data_set_resource_id):

        log.info("_updateDataResource(%s) " % data_set_resource_id)

        dataset_resource = yield self.rc.get_instance(data_set_resource_id)

        log.info("Fetching the data resource manually to find out what's in it")

        initial_resource = yield self._getOneAssociationSubject(dataset_resource,
                                                                HAS_A_ID,
                                                                DATASOURCE_RESOURCE_TYPE_ID)

        data_source_resource_id = initial_resource.ResourceIdentity
        log.info("Fetched data source resource %s" % data_source_resource_id)

        #before
        b4_max_ingest_millis            = initial_resource.max_ingest_millis
        b4_update_interval_seconds      = initial_resource.update_interval_seconds
        b4_update_start_datetime_millis = initial_resource.update_start_datetime_millis
        b4_ion_title                    = initial_resource.ion_title
        #b4_ion_institution_id           = initial_resource.ion_institution_id
        b4_ion_description              = initial_resource.ion_description
        b4_is_public                    = initial_resource.is_public
        log.info("Original values are %d, %d, %d, %s, %s, %s" % (b4_max_ingest_millis,
                                                                 b4_update_interval_seconds,
                                                                 b4_update_start_datetime_millis,
                                                                 #b4_ion_institution_id,
                                                                 b4_ion_title,
                                                                 b4_ion_description,
                                                                 b4_is_public))

        log.info("Updating the resource based on what we found")
        fr_max_ingest_millis            = b4_max_ingest_millis + 1
        fr_update_interval_seconds      = b4_update_interval_seconds + 1
        fr_update_start_datetime_millis = b4_update_start_datetime_millis + 1
        #fr_ion_institution_id           = b4_ion_institution_id + "_updated"
        fr_ion_title                    = b4_ion_title + "_updated"
        fr_ion_description              = b4_ion_description + "_updated"
        fr_is_public                    = not b4_is_public


        log.info("new values will be %d, %d, %d, %s, %s, %s" % (fr_max_ingest_millis,
                                                                fr_update_interval_seconds,
                                                                fr_update_start_datetime_millis,
                                                                #fr_ion_institution_id,
                                                                fr_ion_title,
                                                                fr_ion_description,
                                                                fr_is_public))

        log.info("Creating and wrapping update request message")
        ais_req_msg  = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)        
        update_req_msg  = ais_req_msg.CreateObject(UPDATE_DATA_RESOURCE_REQ_TYPE)
        ais_req_msg.message_parameters_reference = update_req_msg

        #what we want it to be
        update_req_msg.data_set_resource_id         = data_set_resource_id
        update_req_msg.max_ingest_millis            = fr_max_ingest_millis
        update_req_msg.update_interval_seconds      = fr_update_interval_seconds
        update_req_msg.update_start_datetime_millis = fr_update_start_datetime_millis
        #update_req_msg.ion_institution_id           = fr_ion_institution_id
        update_req_msg.ion_title                    = fr_ion_title
        update_req_msg.ion_description              = fr_ion_description
        update_req_msg.is_public                    = fr_is_public

        #actual update call
        result_wrapped = yield self.aisc.updateDataResource(ais_req_msg, MYOOICI_USER_ID)

        log.info("Analyzing results of updateDataResource call")
        if not AIS_RESPONSE_MSG_TYPE == result_wrapped.MessageType:
            self.failUnlessEqual(result_wrapped.MessageType, AIS_RESPONSE_MSG_TYPE,
                                 "updateDataResource had an internal failure: " + result_wrapped.error_str)
        self.failUnlessEqual(200, result_wrapped.result, "deleteDataResource didn't return 200 OK")
        self.failUnlessEqual(1, len(result_wrapped.message_parameters_reference),
                             "updateDataResource returned a GPB with wrong number of 'message_parameters_reference's")

        #unpack result and dig deeper
        result = result_wrapped.message_parameters_reference[0]
        self.failUnlessEqual(result.success, True, "updateDataResource didn't report success")

        #look up resource to compare with fields from original
        updated_resource = yield self.rc.get_instance(data_source_resource_id)
        self.failUnlessEqual(fr_max_ingest_millis             , updated_resource.max_ingest_millis)
        self.failUnlessEqual(fr_update_interval_seconds       , updated_resource.update_interval_seconds)
        self.failUnlessEqual(fr_update_start_datetime_millis  , updated_resource.update_start_datetime_millis)
        #self.failUnlessEqual(fr_ion_institution_id            , updated_resource.ion_institution_id)
        self.failUnlessEqual(fr_ion_title                     , updated_resource.ion_title)
        self.failUnlessEqual(fr_ion_description               , updated_resource.ion_description)

        # jira bug OOIION-15
        self.failUnlessEqual(fr_is_public                     , updated_resource.is_public)


        self.failUnlessEqual(updated_resource.ResourceLifeCycleState, updated_resource.ACTIVE)


    @defer.inlineCallbacks
    def test_deleteDataResource_BadInput(self):
        """
        run through the delete code but don't specify any ids.
        """

        log.info("Trying to call deleteDataResource with the wrong GPB")
        delete_req_msg  = yield self.mc.create_instance(DELETE_DATA_RESOURCE_REQ_TYPE)
        result       = yield self.aisc.deleteDataResource(delete_req_msg, MYOOICI_USER_ID)
        self.failUnlessEqual(result.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "deleteDataResource accepted a GPB that was known to be the wrong type")

        log.info("Trying to call deleteDataResource with an empty GPB")
        ais_req_msg  = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        delete_req_msg  = ais_req_msg.CreateObject(DELETE_DATA_RESOURCE_REQ_TYPE)
        ais_req_msg.message_parameters_reference = delete_req_msg
        result_wrapped = yield self.aisc.deleteDataResource(ais_req_msg, MYOOICI_USER_ID)
        self.failUnlessEqual(result_wrapped.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "deleteDataResource accepted a GPB without a data_source_resource_id list")

# test for deleting an empty list doesn't seem to work because GPBs treat [] and "None" as the same thing
#         log.info("Trying to call deleteDataResource with no ids")
#         delete_req_msg.data_source_resouce_id.append("TEMP")
#         delete_req_msg.data_source_resource_id.pop()
#         result_wrapped = yield self.aisc.deleteDataResource(delete_req_msg)
#         self.failUnlessEqual(result_wrapped.MessageType, AIS_RESPONSE_MSG_TYPE,
#                              "deleteDataResource accepted a GPB without a data_source_resource_id list")

#         log.info("checking number of AIS returns")
#         self.failUnlessEqual(1, len(result_wrapped.message_parameters_reference),
#                              "deleteDataResource returned a GPB with too many 'message_parameters_reference's")

#         log.info("checking number of deleted ids") # (we deleted none, so should be zero!)
#         result = result_wrapped.message_parameters_reference[0]
#         self.failUnlessEqual(0, len(result.successfully_deleted_id),
#                              "We didn't ask for a deletion but apparently one happened...")


    @defer.inlineCallbacks
    def test_deleteDataResourceSample(self):
        """
        @brief try to delete one of the sample data sources
        """
        yield self._deleteDataResource([SAMPLE_PROFILE_DATASET_ID])
        #yield self._deleteDataResource(SAMPLE_STATION_DATA_SOURCE_ID)


    @defer.inlineCallbacks
    def test_deleteDataResourceMultiple(self):
        """
        @brief try to delete 2 of the sample data sources
        """
        #run the create
        create_resp = yield self._createDataResource()

        yield self._deleteDataResource([SAMPLE_PROFILE_DATASET_ID, create_resp.data_set_id])


    @defer.inlineCallbacks
    def test_deleteDataResourceSequential(self):
        """
        @brief try to delete 2 of the sample data sources
        """
        #run the create
        create_resp = yield self._createDataResource()
        yield self._deleteDataResource([SAMPLE_PROFILE_DATASET_ID])
        yield self._deleteDataResource([create_resp.data_set_id])


    @defer.inlineCallbacks
    def _deleteDataResource(self, data_set_ids):


        log.info("Creating and wrapping delete request")
        ais_req_msg  = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        delete_req_msg  = ais_req_msg.CreateObject(DELETE_DATA_RESOURCE_REQ_TYPE)
        ais_req_msg.message_parameters_reference = delete_req_msg

        delete_req_msg.data_set_resource_id.extend(data_set_ids)

        result_wrapped = yield self.aisc.deleteDataResource(ais_req_msg, MYOOICI_USER_ID)

        if not AIS_RESPONSE_MSG_TYPE == result_wrapped.MessageType:
            self.failUnlessEqual(result_wrapped.MessageType, AIS_RESPONSE_MSG_TYPE,
                                 "deleteDataResource had an internal failure: " + result_wrapped.error_str)

        self.failUnlessEqual(200, result_wrapped.result, "deleteDataResource didn't return 200 OK")
        self.failUnlessEqual(1, len(result_wrapped.message_parameters_reference),
                             "deleteDataResource returned a GPB with too many 'message_parameters_reference's")

        result = result_wrapped.message_parameters_reference[0]

        #check number of deleted ids
        result = result_wrapped.message_parameters_reference[0]
        num_deletions = len(result.successfully_deleted_id)
        log.info("Apparently we deleted %d ids" % num_deletions)
        self.failUnlessEqual(len(data_set_ids), num_deletions,
                             "Expected %d deletion(s), got %d" % (len(data_set_ids), num_deletions))

        #check that it's gone
        for dsource_id in result.successfully_deleted_id:
            dsrc = yield self.rc.get_instance(dsource_id)
            log.info("checking successful deletion of data source with id = '%s'" % dsource_id)
            self.failUnlessEqual(dsrc.ResourceLifeCycleState, dsrc.RETIRED,
                                 "deleteDataResource apparently didn't change the datasource to retired")

        for dset_id in data_set_ids:
            dsrc = yield self.rc.get_instance(dset_id)
            log.info("checking successful deletion of data source with id = '%s'" % dset_id)
            self.failIfEqual(dsrc.ResourceLifeCycleState, dsrc.RETIRED,
                                 "deleteDataResource changed the state of the dataset")


        defer.returnValue(None)


    @defer.inlineCallbacks
    def test_createDataResource_BadInput(self):
        log.info("Trying to call createDataResource with the wrong GPB")
        create_req_msg  = yield self.mc.create_instance(CREATE_DATA_RESOURCE_REQ_TYPE)
        result       = yield self.aisc.createDataResource(create_req_msg, MYOOICI_USER_ID)
        self.failUnlessEqual(result.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "createDataResource accepted a GPB that was known to be the wrong type")



    @defer.inlineCallbacks
    def _createDataResource(self):

        ais_req_msg = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        create_req_msg  = ais_req_msg.CreateObject(CREATE_DATA_RESOURCE_REQ_TYPE)
        ais_req_msg.message_parameters_reference = create_req_msg

        #test empty (fields missing) GPB
        yield self._checkCreateFieldAcceptance(ais_req_msg)

        #test full field set but no URLs
        create_req_msg.user_id                       = MYOOICI_USER_ID
        create_req_msg.source_type                   = create_req_msg.SourceType.NETCDF_S
        create_req_msg.request_type                  = create_req_msg.RequestType.DAP
        create_req_msg.ion_description               = "FIXME: description"
        create_req_msg.ion_institution_id            = "FIXME: institution_id"
        create_req_msg.update_start_datetime_millis  = 30000
        create_req_msg.ion_title                     = "some lame title"
        create_req_msg.update_interval_seconds       = 3600
        create_req_msg.is_public                     = False
        create_req_msg.visualization_url             = "http://foo.org/bar"
        log.info("testing with the full set of fields but no URLs")
        yield self._checkCreateFieldAcceptance(ais_req_msg)

        #test too many URLs
        create_req_msg.base_url     = "FIXME"
        create_req_msg.dataset_url  = "http://thredds1.pfeg.noaa.gov/thredds/dodsC/satellite/GR/ssta/1day"
        log.info("testing with too many URLs")
        yield self._checkCreateFieldAcceptance(ais_req_msg)

        create_req_msg.ClearField("base_url")

        #check range on update_start_datetime_millis (OOIION-164)
        create_req_msg.update_start_datetime_millis  = (int(time.time()) + 41536000) * 1000
        result = yield self.aisc.createDataResource(ais_req_msg, MYOOICI_USER_ID)
        self.failUnlessEqual(result.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "createDataResource accepted a update_start_datetime_millis value more than a year in the future")

        #put it back
        create_req_msg.update_start_datetime_millis  = 30000

        #should be ready for actual call that we expect to succeed
        log.info("testing with the call that we expect to succeed")

        result_wrapped = yield self.aisc.createDataResource(ais_req_msg, MYOOICI_USER_ID)

        if not AIS_RESPONSE_MSG_TYPE == result_wrapped.MessageType:
            self.failUnlessEqual(result_wrapped.MessageType, AIS_RESPONSE_MSG_TYPE,
                                 "createDataResource had an internal failure: " + result_wrapped.error_str)
        self.failUnlessEqual(200, result_wrapped.result, "deleteDataResource didn't return 200 OK")
        self.failUnlessEqual(1, len(result_wrapped.message_parameters_reference),
                             "createDataResource returned a GPB with too many 'message_parameters_reference's")

        result = result_wrapped.message_parameters_reference[0]

        log.info("received data_source_id : " + result.data_source_id)
        log.info("received data_set_id    : " + result.data_set_id)
        log.info("received association_id : " + result.association_id)

        log.info("looking up resource we just added")
        datasource_resource = yield self.rc.get_instance(result.data_source_id)

        self.failUnlessEqual(result.data_source_id, datasource_resource.ResourceIdentity,
                             "Resource ID doesn't match what we created the resource from... STRANGE")

        cm = create_req_msg
        dr = datasource_resource

        log.info("comparing resource with fields from original request")
        self.failUnlessEqual(cm.source_type,                   dr.source_type)
        self.failUnlessEqual(cm.request_type,                  dr.request_type)
        self.failUnlessEqual(cm.dataset_url,                   dr.dataset_url)
        self.failUnlessEqual(cm.ion_title,                     dr.ion_title)
        self.failUnlessEqual(cm.ion_description,               dr.ion_description)
        self.failUnlessEqual(cm.ion_institution_id,            dr.ion_institution_id)
        self.failUnlessEqual(cm.update_interval_seconds,       dr.update_interval_seconds)
        self.failUnlessEqual(cm.update_start_datetime_millis,  dr.update_start_datetime_millis)
        self.failUnlessEqual(cm.is_public,                     dr.is_public)


        #test default value for max ingest millis, 1 second less than update interval
        self.failUnlessEqual((cm.update_interval_seconds - 1) * 1000, dr.max_ingest_millis)

        #fixme, check association with cm.user_id ... but resource registry handles this?

        yield self._checkAssociatedQuantities(dr, DATASET_RESOURCE_TYPE_ID, "data set", 1)
        yield self._checkAssociatedQuantities(dr, DATARESOURCE_SCHEDULE_TYPE_ID, "scheduled task", 1)
        
        """
        # The following tests are for manually testing of the 'initial-ingestion subscription' creation/deletion.  
        # These subscriptions are automatically created in ManageDataResource.create() and
        # automatically deleted in NotificationAlertService.handle_update_event() and
        # NotificationAlertService.handle_offline_event().  These subscriptions are set up for email notification only.
        #
        # generate ingestion events to test auto-subscription creation
        pubSupplementAdded = DatasetSupplementAddedEventPublisher(process=self.test_sup) # all publishers/subscribers need a process associated
        yield pubSupplementAdded.initialize()
        yield pubSupplementAdded.activate()

        pubSourceOffline = DatasourceUnavailableEventPublisher(process=self.test_sup) # all publishers/subscribers need a process associated
        yield pubSourceOffline.initialize()
        yield pubSourceOffline.activate()

        # creates the 'successful-ingestion' event notification for us and sends it
        yield pubSupplementAdded.create_and_publish_event(origin="magnet_topic",
                                           dataset_id=result.data_set_id,
                                           datasource_id=result.data_source_id,
                                           title="Unit Testing",
                                           url="Unit Testing",
                                           start_datetime_millis = 10000,
                                           end_datetime_millis = 11000,
                                           number_of_timesteps = 7
                                     )
        # creates the 'failed-ingestion' event notification for us and sends it
        yield pubSourceOffline.create_and_publish_event(origin="magnet_topic",
                                           datasource_id=result.data_source_id,
                                           error_explanation="Unit Testing")
        yield pu.asleep(3.0)
        """

        defer.returnValue(result)


    @defer.inlineCallbacks
    def _checkAssociatedQuantities(self, some_data_resource, type, type_name, expected):
        log.info("checking association between data source and " + type_name)
        associations = yield self._getAssociatedObjects(some_data_resource, HAS_A_ID, DATARESOURCE_SCHEDULE_TYPE_ID)
        self.failUnlessEqual(len(associations), expected,
                             "got %d associated scheduler tasks instead of %d!" % (len(associations), expected))
        defer.returnValue(None)


    @defer.inlineCallbacks
    def _checkCreateFieldAcceptance(self, req_msg):

        result = yield self.aisc.createDataResource(req_msg, MYOOICI_USER_ID)
        self.failUnlessEqual(result.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "createDataResource accepted a GPB that was known to be lacking data")

        defer.returnValue(None)



    @defer.inlineCallbacks
    def _getOneAssociationSubject(self, the_object, the_predicate, the_subject_type):
        """
        @brief get the subject side of an association when you only expect one
        @return id of what you're after
        """

        #can also do subject=
        found = yield self.ac.find_associations(obj=the_object, \
                                                predicate_or_predicates=the_predicate)

        association = None
        for a in found:
            mystery_resource = yield self.rc.get_instance(a.SubjectReference.key)
            mystery_resource_type = mystery_resource.ResourceTypeID.key
            if the_subject_type == mystery_resource_type:
                if not mystery_resource.RETIRED == mystery_resource.ResourceLifeCycleState:
                    #FIXME: if not association is None then we have data inconsistency!
                    association = a

        #this is an error case!
        if None is association:
            defer.returnValue(None)


        the_resource = yield self.rc.get_associated_resource_subject(association)
        defer.returnValue(the_resource)




    @defer.inlineCallbacks
    def _getAssociatedObjects(self, the_subject, the_predicate, the_object_type):
        """
        @brief get the subject side of an association when you only expect one
        @return id of what you're after
        """

        #can also do obj=
        found = yield self.ac.find_associations(subject=the_subject, \
                                                predicate_or_predicates=the_predicate)

        associations = []
        for a in found:
            mystery_resource = yield self.rc.get_instance(a.ObjectReference.key)
            if the_object_type == mystery_resource.ResourceTypeID.key:
                the_resource = yield self.rc.get_associated_resource_object(a)
                associations.append(the_resource)


        defer.returnValue(associations)


