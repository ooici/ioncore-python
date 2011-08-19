#!/usr/bin/env python

"""
@file ion/integration/test_app_integration.py
@test ion.integration.app_integration_service
@author David Everett
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import logging
import ion.util.procutils as pu

from twisted.internet import defer
import time
    
from ion.core.process.process import Process
from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.core.exception import ReceivedApplicationError
from ion.core.data.storage_configuration_utility import COMMIT_CACHE
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, \
                                                                    ANONYMOUS_USER_ID, \
                                                                    MYOOICI_USER_ID

from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceClientError
from ion.services.coi.resource_registry.association_client import AssociationClient, AssociationClientError
from ion.services.dm.distribution.events import DatasetChangeEventPublisher, \
    DatasetChangeEventSubscriber
from ion.core.data import store
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG, ION_AIS_RESOURCES_CFG

from ion.test.iontest import IonTestCase

from ion.integration.ais.app_integration_service import AppIntegrationServiceClient
#from ion.integration.ais.findDataResources import DatasetUpdateEventSubscriber

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE
from ion.integration.ais.ais_object_identifiers import REGISTER_USER_REQUEST_TYPE, \
                                                       UPDATE_USER_PROFILE_REQUEST_TYPE, \
                                                       REGISTER_USER_RESPONSE_TYPE, \
                                                       GET_USER_PROFILE_REQUEST_TYPE, \
                                                       FIND_DATA_RESOURCES_REQ_MSG_TYPE, \
                                                       GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE, \
                                                       CREATE_DOWNLOAD_URL_REQ_MSG_TYPE, \
                                                       GET_RESOURCES_OF_TYPE_REQUEST_TYPE, \
                                                       GET_RESOURCES_OF_TYPE_RESPONSE_TYPE, \
                                                       GET_RESOURCE_TYPES_RESPONSE_TYPE, \
                                                       GET_RESOURCE_REQUEST_TYPE, \
                                                       GET_RESOURCE_RESPONSE_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_REQ_TYPE, \
                                                       FIND_DATA_SUBSCRIPTIONS_REQ_TYPE, \
                                                       DELETE_SUBSCRIPTION_REQ_TYPE, \
                                                       MANAGE_USER_ROLE_REQUEST_TYPE

# Create CDM Type Objects
datasource_type = object_utils.create_type_identifier(object_id=4502, version=1)
dataset_type = object_utils.create_type_identifier(object_id=10001, version=1)
group_type = object_utils.create_type_identifier(object_id=10020, version=1)
dimension_type = object_utils.create_type_identifier(object_id=10018, version=1)
variable_type = object_utils.create_type_identifier(object_id=10024, version=1)
bounded_array_type = object_utils.create_type_identifier(object_id=10021, version=1)
array_structure_type = object_utils.create_type_identifier(object_id=10025, version=1)

attribute_type = object_utils.create_type_identifier(object_id=10017, version=1)
stringArray_type = object_utils.create_type_identifier(object_id=10015, version=1)
float32Array_type = object_utils.create_type_identifier(object_id=10013, version=1)
int32Array_type = object_utils.create_type_identifier(object_id=10009, version=1)

#
# ResourceID for testing create download URL response
#
TEST_RESOURCE_ID = '01234567-8abc-def0-1234-567890123456'
DISPATCHER_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=7002, version=1)

class TestDatasetUpdateEventSubscriber(DatasetChangeEventSubscriber):
    def __init__(self, *args, **kwargs):
        self.msgs = []
        DatasetChangeEventSubscriber.__init__(self, *args, **kwargs)
                
    def ondata(self, data):
        log.error("TestDatasetUpdateEventSubscriber received a message with name: %s",
                  data['content'].name)
        content = data['content']

        if hasattr(content, 'Repository'):
            content.Repository.persistent = True

        self.msgs.append(data)


class AppIntegrationTest(IonTestCase):
   
    """
    Testing Application Integration Service.
    """

    # Set timeout for Trial tests
    timeout = 40
    
    # set to None to turn off timing logging, set to anything else to turn on timing logging
    AnalyzeTiming = None
    
    class TimeStampsClass (object):
        pass
    
    TimeStamps = TimeStampsClass()
    
    def TimeStamp (self):
        TimeNow = time.time()
        TimeStampStr = "(wall time = " + str (TimeNow) + \
                       ", elapse time = " + str(TimeNow - self.TimeStamps.StartTime) + \
                       ", delta time = " + str(TimeNow - self.TimeStamps.LastTime) + \
                       ")"
        self.TimeStamps.LastTime = TimeNow
        return TimeStampStr
    
        
    @defer.inlineCallbacks
    def setUp(self):
        log.debug('AppIntegrationTest.setUp():')
        yield self._start_container()

        store.Store.kvs.clear()
        store.IndexStore.kvs.clear()
        store.IndexStore.indices.clear()
        
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
                'name':'app_integration',
                'module':'ion.integration.ais.app_integration_service',
                'class':'AppIntegrationService'
            },
            {
                'name':'notification_alert',
                'module':'ion.integration.ais.notification_alert_service',
                'class':'NotificationAlertService'
            },
            {
                'name':'dataset_controller',
                'module':'ion.services.dm.inventory.dataset_controller',
                'class':'DatasetControllerClient'
            },

            ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')

        self.sup = sup

        self.aisc = AppIntegrationServiceClient(proc=sup)
        self.rc = ResourceClient(proc=sup)
        self.ac  = AssociationClient(proc=sup)
        self._proc = Process()
        
        if self.AnalyzeTiming != None:
            self.TimeStamps.StartTime = time.time()
            self.TimeStamps.LastTime = self.TimeStamps.StartTime
    


    @defer.inlineCallbacks
    def tearDown(self):
        log.info('Tearing Down Test Container')

        store.Store.kvs.clear()
        store.IndexStore.kvs.clear()
        store.IndexStore.indices.clear()

        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_notificationSet(self):
        """
        Test for the return of notificationSet field: this is in a separate test
        to make it convenient for unit testing.  The notificationSet field is
        actually returned in the findDataResources response, but it will only
        be set if there is a subscription set for a dataset/userID combo, which
        this test scenario sets up.
        """

        log.debug('Testing findDataResources.')

        yield self.createUser()

        #
        # Send a message with no bounds to get a list of dataset ID's; then
        # take one of those IDs and create a subscription on it.
        #
        
        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create a request message 
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.user_ooi_id  = self.user_id
        #reqMsg.message_parameters_reference.user_ooi_id  = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        rspMsg = yield self.aisc.findDataResources(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResources failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].dataResourceSummary)
        log.debug('findDataResources returned: ' + str(numResReturned) + ' resources.')

        self.__validateDataResourceSummary(rspMsg.message_parameters_reference[0].dataResourceSummary)

        if numResReturned > 0:
            log.debug('test_notificationSet: %s datasets returned!' % (numResReturned))
            dsID = rspMsg.message_parameters_reference[0].dataResourceSummary[0].datasetMetadata.data_resource_id

            #
            # Now that we have a valid datasetID, create a subscription for it, and then
            # test to see if the NotificationSet comes back.
            #
    
            log.debug('Calling __createSubscriptions with dsID: ' + dsID + ' and userID: ' + self.user_id)
            yield self.__createSubscriptions(dsID, self.user_id)
            
            log.debug('Calling findDataResources to get list of resources with no bounds to test NotificationSet.')
            # create a request message 
            reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
            reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
            reqMsg.message_parameters_reference.user_ooi_id  = self.user_id
            
            rspMsg = yield self.aisc.findDataResources(reqMsg, self.user_id)
            if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
                self.fail("findDataResources failed: " + rspMsg.error_str)
    
            numResReturned = len(rspMsg.message_parameters_reference[0].dataResourceSummary)
            log.debug('findDataResources returned: ' + str(numResReturned) + ' resources.')
    
            #
            # Validate the fields first
            #
            self.__validateDataResourceSummary(rspMsg.message_parameters_reference[0].dataResourceSummary)
            
            #
            # Now validate that notification has been set on the correct datasetID
            #
            self.__validateNotificationSet(rspMsg.message_parameters_reference[0].dataResourceSummary, dsID)
        else:
            log.error('test_notificationSet: No datasets returned!')
        

    @defer.inlineCallbacks
    def test_findDataResources(self):

        log.debug('Testing findDataResources.')

        yield self.createUser()

        #
        # Send a message with no bounds
        #
        
        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create a request message 
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        
        log.debug('Calling findDataResources to get list of resources with no bounds.')
        rspMsg = yield self.aisc.findDataResources(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResources failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].dataResourceSummary)
        log.debug('findDataResources returned: ' + str(numResReturned) + ' resources.')

        self.__validateDataResourceSummary(rspMsg.message_parameters_reference[0].dataResourceSummary)

        #
        # Send a message with bounds
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        #reqMsg.message_parameters_reference.minLatitude  = 30
        #reqMsg.message_parameters_reference.maxLatitude  = 45
        #reqMsg.message_parameters_reference.minLongitude = -75
        #reqMsg.message_parameters_reference.maxLongitude = -70
        reqMsg.message_parameters_reference.minLongitude = -90
        reqMsg.message_parameters_reference.maxLongitude = -70
        reqMsg.message_parameters_reference.minVertical  = 20
        reqMsg.message_parameters_reference.maxVertical  = 30
        #reqMsg.message_parameters_reference.posVertical  = 'down'
        reqMsg.message_parameters_reference.minTime      = '2011-03-01T00:00:00Z'
        reqMsg.message_parameters_reference.maxTime      = '2011-03-05T00:02:00Z'
        
        log.debug('Calling findDataResources to get list of resources with temporal/spatial bounds.')
        rspMsg = yield self.aisc.findDataResources(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResources failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].dataResourceSummary)
        log.debug('findDataResources returned: ' + str(numResReturned) + ' resources.')

        self.__validateDataResourceSummary(rspMsg.message_parameters_reference[0].dataResourceSummary)

        #
        # Send a message with only depth
        #
        # Use the message client to create a message object
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        reqMsg.message_parameters_reference.minVertical  = 10
        reqMsg.message_parameters_reference.maxVertical  = 20
        reqMsg.message_parameters_reference.posVertical  = 'down'
        
        log.debug('Calling findDataResources to get list of resources bounded by depth only.')
        rspMsg = yield self.aisc.findDataResources(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResources failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].dataResourceSummary)
        log.debug('findDataResources returned: ' + str(numResReturned) + ' resources.')

        self.__validateDataResourceSummary(rspMsg.message_parameters_reference[0].dataResourceSummary)

        #
        # Send a message with only altitude
        #
        # Use the message client to create a message object
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        reqMsg.message_parameters_reference.minVertical  = 10
        reqMsg.message_parameters_reference.maxVertical  = 20
        reqMsg.message_parameters_reference.posVertical  = 'up'
        
        log.debug('Calling findDataResources to get list of resources bounded by altitude only.')
        rspMsg = yield self.aisc.findDataResources(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResources failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].dataResourceSummary)
        log.debug('findDataResources returned: ' + str(numResReturned) + ' resources.')

        self.__validateDataResourceSummary(rspMsg.message_parameters_reference[0].dataResourceSummary)


    @defer.inlineCallbacks
    def test_findDataResourcesByUser(self):

        log.debug('Testing findDataResourcesByUser.')
        #raise unittest.SkipTest('findDataResourcesByUser Skipped.')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        #
        # Send a request without a resourceID to test that the appropriate error
        # is returned.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.user_ooi_id  = 'Dr. Chew'
        reqMsg.message_parameters_reference.minLatitude  = 40.2216682434
        reqMsg.message_parameters_reference.maxLatitude  = 40.2216682434
        reqMsg.message_parameters_reference.minLongitude = -74.13
        reqMsg.message_parameters_reference.maxLongitude = -73.50
        reqMsg.message_parameters_reference.minVertical  = 20
        reqMsg.message_parameters_reference.maxVertical  = 30
        reqMsg.message_parameters_reference.posVertical  = 'down'
        reqMsg.message_parameters_reference.minTime      = '2010-07-26T00:02:00Z'
        reqMsg.message_parameters_reference.maxTime      = '2010-07-26T00:02:00Z'

        log.debug('Calling findDataResourcesByUser without ooi_user_id: should fail.')
        rspMsg = yield self.aisc.findDataResourcesByUser(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('rspMsg to GPB w/missing user_ooi_ID is not an AIS_RESPONSE_ERROR_TYPE GPB')

        #
        # Send a request with a temporal bounds covered by data time 
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.user_ooi_id  = '621F69FC-37C3-421F-8AE9-4D762A2718C9'
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        reqMsg.message_parameters_reference.minLatitude  = -50
        reqMsg.message_parameters_reference.maxLatitude  = -40
        reqMsg.message_parameters_reference.minLongitude = 20
        reqMsg.message_parameters_reference.maxLongitude = 30
        reqMsg.message_parameters_reference.minVertical  = 20
        reqMsg.message_parameters_reference.maxVertical  = 30
        #reqMsg.message_parameters_reference.posVertical  = 'down'
        reqMsg.message_parameters_reference.minTime      = '2010-07-26T00:02:05Z'
        reqMsg.message_parameters_reference.maxTime      = '2010-07-26T00:02:15Z'

        log.debug('Calling findDataResourcesByUser to get list of resources.')
        rspMsg = yield self.aisc.findDataResourcesByUser(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResourcesByUser failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata)
        if numResReturned == 0:
            self.fail('findDataResourcesByUser returned zero resources.')
        
        self.__validateDatasetByOwnerMetadata(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata)

        #
        # Send a request with temporal bounds that covers data time
        # Data Start Time: 2008-08-01T00:50:00Z
        # Data End Time:   2008-08-01T23:50:00Z
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        reqMsg.message_parameters_reference.minLatitude  = -50
        reqMsg.message_parameters_reference.maxLatitude  = -40
        reqMsg.message_parameters_reference.minLongitude = 20
        reqMsg.message_parameters_reference.maxLongitude = 30
        reqMsg.message_parameters_reference.minVertical  = 20
        reqMsg.message_parameters_reference.maxVertical  = 30
        #reqMsg.message_parameters_reference.posVertical  = 'down'
        reqMsg.message_parameters_reference.minTime      = '2010-07-26T00:00:00Z'
        reqMsg.message_parameters_reference.maxTime      = '2010-07-26T02:00:00Z'

        log.debug('Calling findDataResourcesByUser to get list of resources.')
        rspMsg = yield self.aisc.findDataResourcesByUser(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResourcesByUser failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata)
        if numResReturned == 0:
            self.fail('findDataResourcesByUser returned zero resources.')
        
        self.__validateDatasetByOwnerMetadata(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata)

        #
        # Send a request with a temporal bounds minTime covered by data time, but
        # temporal bounds maxTime > dataTime (should still return data)
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        reqMsg.message_parameters_reference.minLatitude  = -50
        reqMsg.message_parameters_reference.maxLatitude  = -40
        reqMsg.message_parameters_reference.minLongitude = 20
        reqMsg.message_parameters_reference.maxLongitude = 30
        reqMsg.message_parameters_reference.minVertical  = 20
        reqMsg.message_parameters_reference.maxVertical  = 30
        #reqMsg.message_parameters_reference.posVertical  = 'down'
        reqMsg.message_parameters_reference.minTime      = '2010-07-26T00:20:00Z'
        reqMsg.message_parameters_reference.maxTime      = '2011-01-1T11:00:00Z'

        log.debug('Calling findDataResourcesByUser to get list of resources.')
        rspMsg = yield self.aisc.findDataResourcesByUser(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResourcesByUser failed: " + rspMsg.error_str)
        
        numResReturned = len(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata)
        if numResReturned == 0:
            self.fail('findDataResourcesByUser returned zero resources.')
        
        self.__validateDatasetByOwnerMetadata(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata)

        #
        # Send a request with a temporal bounds maxTime covered by data time, but
        # temporal bounds minTime < dataTime (should still return data)
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        reqMsg.message_parameters_reference.minLatitude  = -50
        reqMsg.message_parameters_reference.maxLatitude  = -40
        reqMsg.message_parameters_reference.minLongitude = 20
        reqMsg.message_parameters_reference.maxLongitude = 30
        reqMsg.message_parameters_reference.minVertical  = 20
        reqMsg.message_parameters_reference.maxVertical  = 30
        #reqMsg.message_parameters_reference.posVertical  = 'down'
        reqMsg.message_parameters_reference.minTime      = '2009-07-26T00:00:00Z'
        reqMsg.message_parameters_reference.maxTime      = '2010-07-26T01:00:00Z'

        log.debug('Calling findDataResourcesByUser to get list of resources.')
        rspMsg = yield self.aisc.findDataResourcesByUser(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResourcesByUser failed: " + rspMsg.error_str)
        
        numResReturned = len(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata)
        if numResReturned == 0:
            self.fail('findDataResourcesByUser returned zero resources.')
        
        self.__validateDatasetByOwnerMetadata(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata)


    @defer.inlineCallbacks
    def test_getDataResourceDetail(self):

        log.debug('Testing getDataResourceDetail.')

        #
        # Create a message client
        #
        mc = MessageClient(proc=self.test_sup)

        #
        # Send a request without a resourceID to test that the appropriate error
        # is returned.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE)

        log.debug('Calling getDataResourceDetail without resource ID.')
        rspMsg = yield self.aisc.getDataResourceDetail(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('rspMsg to GPB w/missing resource ID is not an AIS_RESPONSE_ERROR_TYPE GPB')
        
        #
        # In order to test getDataResourceDetail, we need a dataset resource
        # ID.  So, first use findDataResources to get the instances of data
        # resources that match some test criteria, and the first resource ID
        # out of the results.
        #
        log.debug('DHE: AppIntegrationService! instantiating FindResourcesMsg.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = ANONYMOUS_USER_ID
        
        log.debug('Calling findDataResources.')
        rspMsg = yield self.aisc.findDataResources(reqMsg, ANONYMOUS_USER_ID)

        if len(rspMsg.message_parameters_reference) > 0:
            if len(rspMsg.message_parameters_reference[0].dataResourceSummary) > 0:
                dsID = rspMsg.message_parameters_reference[0].dataResourceSummary[0].datasetMetadata.data_resource_id
        
                #
                # Now create a request message to get the metadata details about the
                # source (i.e., where the dataset came from) of a particular dataset
                # resource ID.
                #
                reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
                reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE)
                reqMsg.message_parameters_reference.data_resource_id = dsID

                log.debug('Calling getDataResourceDetail.')
                rspMsg = yield self.aisc.getDataResourceDetail(reqMsg, ANONYMOUS_USER_ID)
                log.debug('getDataResourceDetail returned:\n' + \
                    str('resource_id: ') + \
                    str(rspMsg.message_parameters_reference[0].data_resource_id) + \
                    str('\n'))

                dSource = rspMsg.message_parameters_reference[0].source
                log.debug('Source Metadata for Dataset:\n')
                for property in dSource.property:
                    log.debug('  Property: ' + property)
                for station_id in dSource.station_id:
                    log.debug('  Station_ID: ' + station_id)
 
                log.debug('  RequestType: ' + str(dSource.request_type))
                log.debug('  Base URL: ' + dSource.base_url)
                log.debug('  Max Ingest Millis: ' + str(dSource.max_ingest_millis))
                log.debug('  ion_title: ' + dSource.ion_title)
                log.debug('  ion_description: ' + dSource.ion_description)
                log.debug('  ion_name: ' + dSource.ion_name)
                log.debug('  ion_email: ' + dSource.ion_email)
                log.debug('  ion_institution: ' + dSource.ion_institution)

                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('title'):
                    #self.fail('response to findDataResources has no title field')
                    log.error('response to findDataResources has no title field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('institution'):
                    #self.fail('response to findDataResources has no institution field')
                    log.error('response to findDataResources has no institution field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('source'):
                    #self.fail('response to findDataResources has no source field')
                    log.error('response to findDataResources has no source field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('references'):
                    #self.fail('response to findDataResources has no references field')
                    log.error('response to findDataResources has no references field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_time_coverage_start'):
                    self.fail('response to findDataResources has no ion_time_coverage_start field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_time_coverage_end'):
                    self.fail('response to findDataResources has no ion_time_coverage_end field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('summary'):
                    #self.fail('response to findDataResources has no summary field')
                    log.error('response to findDataResources has no summary field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('comment'):
                    #self.fail('response to findDataResources has no comment field')
                    log.error('response to findDataResources has no comment field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_geospatial_lat_min'):
                    self.fail('response to findDataResources has no ion_geospatial_lat_min field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_geospatial_lat_max'):
                    self.fail('response to findDataResources has no ion_geospatial_lat_max field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_geospatial_lon_min'):
                    self.fail('response to findDataResources has no ion_geospatial_lon_min field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_geospatial_lon_max'):
                    self.fail('response to findDataResources has no ion_geospatial_lon_max field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_geospatial_vertical_min'):
                    self.fail('response to findDataResources has no ion_geospatial_vertical_min field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_geospatial_vertical_max'):
                    self.fail('response to findDataResources has no ion_geospatial_vertical_max field')
                if not rspMsg.message_parameters_reference[0].dataResourceSummary.IsFieldSet('ion_geospatial_vertical_positive'):
                    self.fail('response to findDataResources has no ion_geospatial_vertical_positive field')

                self.__printMetadata(rspMsg)

        
    @defer.inlineCallbacks
    def test_createDownloadURL(self):

        log.debug('Testing createDownloadURL')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        #
        # Send a request without a resourceID to test that the appropriate error
        # is returned.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(CREATE_DOWNLOAD_URL_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = 'Dr. Chew'

        log.debug('Calling createDownloadURL without resource ID.')
        rspMsg = yield self.aisc.createDownloadURL(reqMsg, reqMsg.message_parameters_reference.user_ooi_id)
        if rspMsg.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('rspMsg to GPB w/missing resource ID is not an AIS_RESPONSE_ERROR_TYPE GPB')
        
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(CREATE_DOWNLOAD_URL_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = 'Dr. Chew'
        reqMsg.message_parameters_reference.data_resource_id = TEST_RESOURCE_ID

        log.debug('Calling createDownloadURL.')
        rspMsg = yield self.aisc.createDownloadURL(reqMsg, reqMsg.message_parameters_reference.user_ooi_id)
        downloadURL = rspMsg.message_parameters_reference[0].download_url
        log.debug('DHE: createDownloadURL returned:\n' + downloadURL)
        if TEST_RESOURCE_ID not in downloadURL:
            self.fail("createDownloadURL response does not contain resourceID")


    @defer.inlineCallbacks
    def test_registerUser(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS RegisterUser request')
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_REQUEST_TYPE)
        
        # fill in the certificate and key
        msg.message_parameters_reference.certificate = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        msg.message_parameters_reference.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtbg0kKLmivgoVsA4U7swNDRH6svW24
2THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq7LWt2T6GVVA10ex5WAeB/o7br/Z4
U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b2lUtQc6cjuHRDU4NknXaVMXTBHKP
M40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4dszsqn2SC8YDw1xrujvW2Bd7Q7Bw
MQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+6M6SMQIDAQABAoIBAAc/Ic97ZDQ9
tFh76wzVWj4SVRuxj7HWSNQ+Uzi6PKr8Zy182Sxp74+TuN9zKAppCQ8LEKwpkKtEjXsl8QcXn38m
sXOo8+F1He6FaoRQ1vXi3M1boPpefWLtyZ6rkeJw6VP3MVG5gmho0VaOqLieWKLP6fXgZGUhBvFm
yxUPoNgXJPLjJ9pNGy4IBuQDudqfJeqnbIe0GOXdB1oLCjAgZlTR4lFA92OrkMEldyVp72iYbffN
4GqoCEiHi8lX9m2kvwiQKRnfH1dLnnPBrrwatu7TxOs02HpJ99wfzKRy4B1SKcB0Gs22761r+N/M
oO966VxlkKYTN+soN5ID9mQmXJkCgYEA/h2bqH9mNzHhzS21x8mC6n+MTyYYKVlEW4VSJ3TyMKlR
gAjhxY/LUNeVpfxm2fY8tvQecWaW3mYQLfnvM7f1FeNJwEwIkS/yaeNmcRC6HK/hHeE87+fNVW/U
ftU4FW5Krg3QIYxcTL2vL3JU4Auu3E/XVcx0iqYMGZMEEDOcQPcCgYEA6sLLIeOdngUvxdA4KKEe
qInDpa/coWbtAlGJv8NueYTuD3BYJG5KoWFY4TVfjQsBgdxNxHzxb5l9PrFLm9mRn3iiR/2EpQke
qJzs87K0A/sxTVES29w1PKinkBkdu8pNk10TxtRUl/Ox3fuuZPvyt9hi5c5O/MCKJbjmyJHuJBcC
gYBiAJM2oaOPJ9q4oadYnLuzqms3Xy60S6wUS8+KTgzVfYdkBIjmA3XbALnDIRudddymhnFzNKh8
rwoQYTLCVHDd9yFLW0d2jvJDqiKo+lV8mMwOFP7GWzSSfaWLILoXcci1ZbheJ9607faxKrvXCEpw
xw36FfbgPfeuqUdI5E6fswKBgFIxCu99gnSNulEWemL3LgWx3fbHYIZ9w6MZKxIheS9AdByhp6px
lt1zeKu4hRCbdtaha/TMDbeV1Hy7lA4nmU1s7dwojWU+kSZVcrxLp6zxKCy6otCpA1aOccQIlxll
Vc2vO7pUIp3kqzRd5ovijfMB5nYwygTB4FwepWY5eVfXAoGBAIqrLKhRzdpGL0Vp2jwtJJiMShKm
WJ1c7fBskgAVk8jJzbEgMxuVeurioYqj0Cn7hFQoLc+npdU5byRti+4xjZBXSmmjo4Y7ttXGvBrf
c2bPOQRAYZyD2o+/MHBDsz7RWZJoZiI+SJJuE4wphGUsEbI2Ger1QW9135jKp6BsY2qZ
-----END RSA PRIVATE KEY-----"""

        # try to register this user for the first time
        reply = yield self.aisc.registerUser(msg, ANONYMOUS_USER_ID)
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != REGISTER_USER_RESPONSE_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        if reply.message_parameters_reference[0].user_already_registered != False:
            self.fail("response does not indicate user wasn't already registered")
        if reply.message_parameters_reference[0].user_is_admin != False:
            self.fail("response indicates user is administrator")
        if reply.message_parameters_reference[0].user_is_early_adopter != True:
            self.fail("response does not indicate user is an early adopter")
        FirstOoiId = reply.message_parameters_reference[0].ooi_id
        log.info("test_registerUser: first time registration received GPB = "+str(reply.message_parameters_reference[0]))
            
        # try to re-register this user for a second time
        reply = yield self.aisc.registerUser(msg, ANONYMOUS_USER_ID)
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != REGISTER_USER_RESPONSE_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        if reply.message_parameters_reference[0].user_already_registered != True:
            self.fail("response does not indicate user was already registered")
        if reply.message_parameters_reference[0].user_is_admin != False:
            self.fail("response indicates user is administrator")
        if reply.message_parameters_reference[0].user_is_early_adopter != True:
            self.fail("response does not indicate user is an early adopter")
        if FirstOoiId != reply.message_parameters_reference[0].ooi_id:
            self.fail("re-registration did not return the same OoiId as registration")
        log.info("test_registerUser: re-registration received GPB = "+str(reply.message_parameters_reference[0]))
        
        # try to send registerUser the wrong GPB
        # create a bad request GPBs
        msg = yield mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.registerUser(msg, ANONYMOUS_USER_ID)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB is not an AIS_RESPONSE_ERROR_TYPE GPB')

        # try to send registerUser incomplete GPBs
        # create a bad GPB request w/o payload
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.registerUser(msg, ANONYMOUS_USER_ID)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to registerUser is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o certificate
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_REQUEST_TYPE)
        reply = yield self.aisc.registerUser(msg, ANONYMOUS_USER_ID)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to registerUser is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o key
        msg.message_parameters_reference.certificate = "dumming certificate"
        reply = yield self.aisc.registerUser(msg, ANONYMOUS_USER_ID)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to registerUser is not an AIS_RESPONSE_ERROR_TYPE GPB')
            
    @defer.inlineCallbacks
    def test_updateUserProfile_getUser(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        # comment these tests out until updateUserProfile is added to the policy service
        # test for authentication policy failure
        # create the update Email request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS updateUserProfile request')
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_PROFILE_REQUEST_TYPE)
        msg.message_parameters_reference.user_ooi_id = "ANONYMOUS"
        msg.message_parameters_reference.name = "some_person"
        msg.message_parameters_reference.institution = "some_place"
        msg.message_parameters_reference.email_address = "some_person@some_place.some_domain"
        try:
            reply = yield self.aisc.updateUserProfile(msg, msg.message_parameters_reference.user_ooi_id)
            self.fail('updateUserProfile did not raise exception for ANONYMOUS ooi_id')
        except ReceivedApplicationError:
            log.info("updateUserProfile correctly raised exception for ANONYMOUS ooi_id")
            
        # create the getUser request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS updateUserProfile request')
        msg.message_parameters_reference = msg.CreateObject(GET_USER_PROFILE_REQUEST_TYPE)
        msg.message_parameters_reference.user_ooi_id = "ANONYMOUS"
        try:
            reply = yield self.aisc.getUser(msg, msg.message_parameters_reference.user_ooi_id)
            self.fail('getUser did not raise exception for ANONYMOUS ooi_id')
        except ReceivedApplicationError:
            log.info("getUser correctly raised exception for ANONYMOUS ooi_id")
        
        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS RegisterUser request')
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_REQUEST_TYPE)
        
        # fill in the certificate and key
        msg.message_parameters_reference.certificate = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        msg.message_parameters_reference.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtbg0kKLmivgoVsA4U7swNDRH6svW24
2THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq7LWt2T6GVVA10ex5WAeB/o7br/Z4
U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b2lUtQc6cjuHRDU4NknXaVMXTBHKP
M40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4dszsqn2SC8YDw1xrujvW2Bd7Q7Bw
MQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+6M6SMQIDAQABAoIBAAc/Ic97ZDQ9
tFh76wzVWj4SVRuxj7HWSNQ+Uzi6PKr8Zy182Sxp74+TuN9zKAppCQ8LEKwpkKtEjXsl8QcXn38m
sXOo8+F1He6FaoRQ1vXi3M1boPpefWLtyZ6rkeJw6VP3MVG5gmho0VaOqLieWKLP6fXgZGUhBvFm
yxUPoNgXJPLjJ9pNGy4IBuQDudqfJeqnbIe0GOXdB1oLCjAgZlTR4lFA92OrkMEldyVp72iYbffN
4GqoCEiHi8lX9m2kvwiQKRnfH1dLnnPBrrwatu7TxOs02HpJ99wfzKRy4B1SKcB0Gs22761r+N/M
oO966VxlkKYTN+soN5ID9mQmXJkCgYEA/h2bqH9mNzHhzS21x8mC6n+MTyYYKVlEW4VSJ3TyMKlR
gAjhxY/LUNeVpfxm2fY8tvQecWaW3mYQLfnvM7f1FeNJwEwIkS/yaeNmcRC6HK/hHeE87+fNVW/U
ftU4FW5Krg3QIYxcTL2vL3JU4Auu3E/XVcx0iqYMGZMEEDOcQPcCgYEA6sLLIeOdngUvxdA4KKEe
qInDpa/coWbtAlGJv8NueYTuD3BYJG5KoWFY4TVfjQsBgdxNxHzxb5l9PrFLm9mRn3iiR/2EpQke
qJzs87K0A/sxTVES29w1PKinkBkdu8pNk10TxtRUl/Ox3fuuZPvyt9hi5c5O/MCKJbjmyJHuJBcC
gYBiAJM2oaOPJ9q4oadYnLuzqms3Xy60S6wUS8+KTgzVfYdkBIjmA3XbALnDIRudddymhnFzNKh8
rwoQYTLCVHDd9yFLW0d2jvJDqiKo+lV8mMwOFP7GWzSSfaWLILoXcci1ZbheJ9607faxKrvXCEpw
xw36FfbgPfeuqUdI5E6fswKBgFIxCu99gnSNulEWemL3LgWx3fbHYIZ9w6MZKxIheS9AdByhp6px
lt1zeKu4hRCbdtaha/TMDbeV1Hy7lA4nmU1s7dwojWU+kSZVcrxLp6zxKCy6otCpA1aOccQIlxll
Vc2vO7pUIp3kqzRd5ovijfMB5nYwygTB4FwepWY5eVfXAoGBAIqrLKhRzdpGL0Vp2jwtJJiMShKm
WJ1c7fBskgAVk8jJzbEgMxuVeurioYqj0Cn7hFQoLc+npdU5byRti+4xjZBXSmmjo4Y7ttXGvBrf
c2bPOQRAYZyD2o+/MHBDsz7RWZJoZiI+SJJuE4wphGUsEbI2Ger1QW9135jKp6BsY2qZ
-----END RSA PRIVATE KEY-----"""

        # try to register this user for the first time
        reply = yield self.aisc.registerUser(msg, ANONYMOUS_USER_ID)
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != REGISTER_USER_RESPONSE_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        FirstOoiId = reply.message_parameters_reference[0].ooi_id
        log.info("test_registerUser: first time registration received GPB = "+str(reply.message_parameters_reference[0]))
        
        # create the update Profile request GPBs for setting the email address
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS updateUserProfile request')
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_PROFILE_REQUEST_TYPE)
        msg.message_parameters_reference.user_ooi_id = FirstOoiId
        msg.message_parameters_reference.name = "some_person"
        msg.message_parameters_reference.institution = "some_place"
        msg.message_parameters_reference.email_address = "some_person@some_place.some_domain"
        try:
            reply = yield self.aisc.updateUserProfile(msg, msg.message_parameters_reference.user_ooi_id)
        except ReceivedApplicationError:
            self.fail('updateUserProfile incorrectly raised exception for an authenticated ooi_id')
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('updateUserProfile returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # create the update Profile request GPBs for setting the profile
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS updateUserProfile request')
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_PROFILE_REQUEST_TYPE)
        msg.message_parameters_reference.user_ooi_id = FirstOoiId
        msg.message_parameters_reference.profile.add()
        msg.message_parameters_reference.profile[0].name = "ProfileItem_1_Name"
        msg.message_parameters_reference.profile[0].value = "ProfileItem_1_Value"
        msg.message_parameters_reference.profile.add()
        msg.message_parameters_reference.profile[1].name = "ProfileItem_2_Name"
        msg.message_parameters_reference.profile[1].value = "ProfileItem_2_Value"
        try:
            reply = yield self.aisc.updateUserProfile(msg, msg.message_parameters_reference.user_ooi_id)
        except ReceivedApplicationError:
            self.fail('updateUserProfile incorrectly raised exception for an authenticated ooi_id')
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('updateUserProfile returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
            
        # test that the email & profile got set
        # create the getUser request GPBs for getting the email/profile
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS getUser request')
        msg.message_parameters_reference = msg.CreateObject(GET_USER_PROFILE_REQUEST_TYPE)
        msg.message_parameters_reference.user_ooi_id = FirstOoiId
        try:
            reply = yield self.aisc.getUser(msg, msg.message_parameters_reference.user_ooi_id)
        except ReceivedApplicationError:
            self.fail('getUser incorrectly raised exception for an authenticated ooi_id')
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getUser returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response from getUser is not an AIS_RESPONSE_MSG_TYPE GPB')
        self.assertEqual(reply.message_parameters_reference[0].name, "some_person")
        self.assertEqual(reply.message_parameters_reference[0].institution, "some_place")
        self.assertEqual(reply.message_parameters_reference[0].email_address, "some_person@some_place.some_domain")
        self.assertEqual(reply.message_parameters_reference[0].authenticating_organization, "ProtectNetwork")
        self.assertEqual(reply.message_parameters_reference[0].profile[0].name, "ProfileItem_1_Name")
        self.assertEqual(reply.message_parameters_reference[0].profile[0].value, "ProfileItem_1_Value")
        self.assertEqual(reply.message_parameters_reference[0].profile[1].name, "ProfileItem_2_Name")
        self.assertEqual(reply.message_parameters_reference[0].profile[1].value, "ProfileItem_2_Value")


        # try to send updateUserProfile the wrong GPB
        # create a bad request GPBs
        msg = yield mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.updateUserProfile(msg, ANONYMOUS_USER_ID)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserProfile is not an AIS_RESPONSE_ERROR_TYPE GPB')

        # try to send updateUserProfile incomplete GPBs
        # create a bad GPB request w/o payload
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.updateUserProfile(msg, ANONYMOUS_USER_ID)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserProfile to is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o ooi_id
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_PROFILE_REQUEST_TYPE)
        reply = yield self.aisc.updateUserProfile(msg, msg.message_parameters_reference.user_ooi_id)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserProfile is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o emsil address
        msg.message_parameters_reference.user_ooi_id = "Some-ooi_id"
        reply = yield self.aisc.updateUserProfile(msg, ANONYMOUS_USER_ID)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserProfile is not an AIS_RESPONSE_ERROR_TYPE GPB')

    @defer.inlineCallbacks
    def test_setUserRole(self):
        log.debug('Testing setUserRole')

        valid_ooi_id = 'A7B44115-34BC-4553-B51E-1D87617F12E0'

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(MANAGE_USER_ROLE_REQUEST_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = valid_ooi_id

        rspMsg = yield self.aisc.setUserRole(reqMsg, reqMsg.message_parameters_reference.user_ooi_id)
        if rspMsg.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('rspMsg to GPB w/missing role is not an AIS_RESPONSE_ERROR_TYPE GPB')

        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(MANAGE_USER_ROLE_REQUEST_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = valid_ooi_id
        reqMsg.message_parameters_reference.role = 'AUTHENTICATED'

        rspMsg = yield self.aisc.setUserRole(reqMsg, reqMsg.message_parameters_reference.user_ooi_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('rspMsg to GPB w/ valid data is an AIS_RESPONSE_ERROR_TYPE GPB')

    @defer.inlineCallbacks
    def test_getResourceTypes(self):

        ResourceTypes = ['datasets', 'identities', 'datasources', 'epucontrollers']
        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        # create the empty request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS getResourceTypes request')
        reply = yield self.aisc.getResourceTypes(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResourceTypes returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        log.debug('getResourceTypes returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCE_TYPES_RESPONSE_TYPE:
            self.fail('response to getResourceTypes is not a GET_RESOURCE_TYPES_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('resource_types_list'):
            self.fail('response to getResourceTypes has no resource_types_list field')
        if len(ResourceTypes) != len(reply.message_parameters_reference[0].resource_types_list):
            self.fail('response to getResourceTypes has incorrect number of resource types')
        for Type in reply.message_parameters_reference[0].resource_types_list:
            if Type not in ResourceTypes:
                self.fail('response to getResourceTypes has unexpected resource type [%s]'%Type)
 
    @defer.inlineCallbacks
    def test_getResourcesOfType(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        # create the getResourcesOfType request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS getResourcesOfType request')
        msg.message_parameters_reference = msg.CreateObject(GET_RESOURCES_OF_TYPE_REQUEST_TYPE)
        
        msg.message_parameters_reference.resource_type = "datasets"
        log.debug('getResourcesOfType: calling AIS to get datasets')
        reply = yield self.aisc.getResourcesOfType(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResourcesOfType returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCES_OF_TYPE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCES_OF_TYPE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('column_names'):
            self.fail('response to getResourcesOfType has no column_names field')
        if not reply.message_parameters_reference[0].IsFieldSet('resources'):
            self.fail('response to getResourcesOfType has no resources field')
        log.debug('getResourcesOfType returned column_names:\n'+str(reply.message_parameters_reference[0].column_names))
        log.debug('getResourcesOfType returned resources:\n')
        for resource in reply.message_parameters_reference[0].resources:
           log.debug(str(resource))
        
        msg.message_parameters_reference.resource_type = "identities"
        log.debug('getResourcesOfType: calling AIS to get identities')
        reply = yield self.aisc.getResourcesOfType(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResourcesOfType returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCES_OF_TYPE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCES_OF_TYPE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('column_names'):
            self.fail('response to getResourcesOfType has no column_names field')
        if not reply.message_parameters_reference[0].IsFieldSet('resources'):
            self.fail('response to getResourcesOfType has no resources field')
        log.debug('getResourcesOfType returned column_names:\n'+str(reply.message_parameters_reference[0].column_names))
        log.debug('getResourcesOfType returned resources:\n')
        for resource in reply.message_parameters_reference[0].resources:
           log.debug(str(resource))
        
        msg.message_parameters_reference.resource_type = "datasources"
        log.debug('getResourcesOfType: calling AIS to get datasources')
        reply = yield self.aisc.getResourcesOfType(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResourcesOfType returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCES_OF_TYPE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCES_OF_TYPE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('column_names'):
            self.fail('response to getResourcesOfType has no column_names field')
        if not reply.message_parameters_reference[0].IsFieldSet('resources'):
            self.fail('response to getResourcesOfType has no resources field')
        log.debug('getResourcesOfType returned column_names:\n'+str(reply.message_parameters_reference[0].column_names))
        log.debug('getResourcesOfType returned resources:\n')
        for resource in reply.message_parameters_reference[0].resources:
           log.debug(str(resource))
        
        msg.message_parameters_reference.resource_type = "epucontrollers"
        log.debug('getResourcesOfType: calling AIS to get epucontrollers')
        reply = yield self.aisc.getResourcesOfType(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResourcesOfType returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCES_OF_TYPE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCES_OF_TYPE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('column_names'):
            self.fail('response to getResourcesOfType has no column_names field')
        if not reply.message_parameters_reference[0].IsFieldSet('resources'):
            self.fail('response to getResourcesOfType has no resources field')
        log.debug('getResourcesOfType returned column_names:\n'+str(reply.message_parameters_reference[0].column_names))
        log.debug('getResourcesOfType returned resources:\n')
        for resource in reply.message_parameters_reference[0].resources:
           log.debug(str(resource))

    @defer.inlineCallbacks
    def test_getResource(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        # create the getResources request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS getResource request')
        msg.message_parameters_reference = msg.CreateObject(GET_RESOURCE_REQUEST_TYPE)
        
        msg.message_parameters_reference.ooi_id = "agentservices_epu_controller"  #epu controller
        reply = yield self.aisc.getResource(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResource returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        log.debug('getResource returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('resource'):
            self.fail('response to getResourcesOfType has no resource field')
        log.debug('getResource returned:\n')
        for item in reply.message_parameters_reference[0].resource:
            log.debug(str(item)+'\n')
        
        msg.message_parameters_reference.ooi_id = "3319A67F-81F3-424F-8E69-4F28C4E047F1"  #data set
        reply = yield self.aisc.getResource(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResource returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        log.debug('getResource returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('resource'):
            self.fail('response to getResourcesOfType has no resource field')
        log.debug('getResource returned:\n')
        for item in reply.message_parameters_reference[0].resource:
            log.debug(str(item)+'\n')
        
        msg.message_parameters_reference.ooi_id = "A3D5D4A0-7265-4EF2-B0AD-3CE2DC7252D8"   #anonymous identity
        reply = yield self.aisc.getResource(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResource returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        log.debug('getResource returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('resource'):
            self.fail('response to getResourcesOfType has no resource field')
        log.debug('getResource returned:\n')
        for item in reply.message_parameters_reference[0].resource:
            log.debug(str(item)+'\n')
        
        msg.message_parameters_reference.ooi_id = "3319A67F-91F3-424F-8E69-4F28C4E047F2"  #data source
        reply = yield self.aisc.getResource(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResource returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        log.debug('getResource returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('resource'):
            self.fail('response to getResourcesOfType has no resource field')
        log.debug('getResource returned:\n')
        for item in reply.message_parameters_reference[0].resource:
            log.debug(str(item)+'\n')
        
        msg.message_parameters_reference.ooi_id = "bogus-ooi_id"  #non-existant item
        reply = yield self.aisc.getResource(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResource returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad ooi_id is not an AIS_RESPONSE_ERROR_TYPE GPB')

        yield self.createUser()        
        msg.message_parameters_reference.ooi_id = self.user_id   #created identity
        reply = yield self.aisc.getResource(msg, MYOOICI_USER_ID)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('getResource returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != GET_RESOURCE_RESPONSE_TYPE:
            self.fail('response to getResourcesOfType is not a GET_RESOURCE_RESPONSE_TYPE GPB')           
        if not reply.message_parameters_reference[0].IsFieldSet('resource'):
            self.fail('response to getResourcesOfType has no resource field')
        log.debug('getResource returned:\n')
        for item in reply.message_parameters_reference[0].resource:
            log.debug(str(item)+'\n')


    @defer.inlineCallbacks
    def __register_dispatcher(self, name):
        dispatcher_res = yield self.rc.create_instance(DISPATCHER_RESOURCE_TYPE, ResourceName=name)
        dispatcher_res.dispatcher_name = name
        yield self.rc.put_instance(dispatcher_res, 'Committing new dispatcher resource for registration')
        defer.returnValue(dispatcher_res)


    @defer.inlineCallbacks
    def test_createDataResourceSubscription(self):
        log.debug('Testing createDataResourcesSubscription.')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        yield self.createUser()

        #
        # Send a request with wrong message type to test that the appropriate error is returned.
        #
        reqMsg = yield mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        log.debug('Calling createDataResourceSubscription with wrong message type.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription with wrong message type')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription with wrong message type')
            
        #
        # Send a request without message_parameters_reference to test that the appropriate error is returned.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        log.debug('Calling createDataResourceSubscription without message_parameters_reference.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription without message_parameters_reference')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription without message_parameters_reference')
            
        #
        # Send a request without subscriptionInfo to test that the appropriate error is returned.
        #
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        log.debug('Calling createDataResourceSubscription without subscriptionInfo.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription without subscriptionInfo')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription without subscriptionInfo')
            
        #
        # Send a request without datasetMetadata to test that the appropriate error is returned.
        #
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        log.debug('Calling createDataResourceSubscription without datasetMetadata.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription without datasetMetadata')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription without datasetMetadata')
            
        #
        # Send a request without subscriptionInfo.user_ooi_id to test that the appropriate error is returned.
        #
        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id    # setting something in datasetMetadata
        log.debug('Calling createDataResourceSubscription without user_ooi_id.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription without user_ooi_id')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription without user_ooi_id')
            
        #
        # Send a request without data_src_id to test that the appropriate error is returned.
        #
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        log.debug('Calling createDataResourceSubscription without data_src_id.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription without data_src_id')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription without data_src_id')
            
        #
        # Send a request without subscription_type to test that the appropriate error is returned.
        #
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        log.debug('Calling createDataResourceSubscription without subscription_type.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription without subscription_type')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription without subscription_type')
            
        #
        # Send a request with bad user_id to test that the appropriate error is returned.
        #
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = "bugus_user_id"
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0
        log.debug('Calling createDataResourceSubscription with bad user_id.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription with bad user_id')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription with bad user_id')
            
        #
        # Send a request with mis-matched info to test that the appropriate error is returned.
        #
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset321'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0
        log.debug('Calling createDataResourceSubscription with mis-matched info .')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription with mis-matched info ')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription with mis-matched info ')
            
        #
        # Send a request with non-existant dispatcher to test that the appropriate error is returned.
        #
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0
        log.debug('Calling createDataResourceSubscription with non-existant dispatcher .')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to createDataResourceSubscription with non-existant dispatcher ')
        else:
            self.fail('POSITIVE rspMsg to createDataResourceSubscription with non-existant dispatcher ')
            
        #
        # Create dispatchers and associations
        #
        log.info("Creating 2 dispatchers for testing")
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource1')
        log.info('Created Dispatcher1 ID: ' + self.dispatcherRes.ResourceIdentity)
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource2')
        self.dispatcherID = self.dispatcherRes.ResourceIdentity
        log.info('Created Dispatcher2 ID: ' + self.dispatcherID)
            
        #
        # Now make an association between the user and this dispatcher
        #
        try:
            log.info('Getting user resource instance')
            try:
                self.userRes = yield self.rc.get_instance(self.user_id)
            except ResourceClientError:
                self.fail('Error getting instance of userID: ' + self.user_id)
            log.info('Got user resource instance: ' + self.userRes.ResourceIdentity)
            association = yield self.ac.create_association(self.userRes, HAS_A_ID, self.dispatcherRes)
            if association not in self.userRes.ResourceAssociationsAsSubject:
                self.fail('Error: subject not in association!')
            if association not in self.dispatcherRes.ResourceAssociationsAsObject:
                self.fail('Error: object not in association')
            
            #
            # Put the association in datastore
            #
            log.debug('Storing association: ' + str(association))
            yield self.rc.put_instance(association)

        except AssociationClientError, ex:
            self.fail('Error creating assocation between userID: ' + self.userID + ' and dispatcherID: ' + self.dispatcherID + '. ex: ' + ex)

        # Add a subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0

        #
        #
        log.debug('Calling createDataResourceSubscription.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to createDataResourceSubscription')
        else:
            log.debug('POSITIVE rspMsg to createDataResourceSubscription')


    @defer.inlineCallbacks
    def test_findDataResourceSubscriptions(self):
        log.debug('Testing findDataResourceSubscriptions.')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        #
        # create a user
        #
        yield self.createUser()

        #
        # Create dispatchers and associations
        #
        log.info("Creating 2 dispatchers for testing")
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource1')
        log.info('Created Dispatcher1 ID: ' + self.dispatcherRes.ResourceIdentity)
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource2')
        self.dispatcherID = self.dispatcherRes.ResourceIdentity
        log.info('Created Dispatcher2 ID: ' + self.dispatcherID)
            
        #
        # Now make an association between the user and the last created dispatcher
        #
        try:
            log.info('Getting user resource instance')
            try:
                self.userRes = yield self.rc.get_instance(self.user_id)
            except ResourceClientError:
                self.fail('Error getting instance of userID: ' + self.user_id)
            log.info('Got user resource instance: ' + self.userRes.ResourceIdentity)
            association = yield self.ac.create_association(self.userRes, HAS_A_ID, self.dispatcherRes)
            if association not in self.userRes.ResourceAssociationsAsSubject:
                self.fail('Error: subject not in association!')
            if association not in self.dispatcherRes.ResourceAssociationsAsObject:
                self.fail('Error: object not in association')           
            # Put the association in datastore
            log.debug('Storing association: ' + str(association))
            yield self.rc.put_instance(association)
        except AssociationClientError, ex:
            self.fail('Error creating assocation between userID: ' + self.userID + ' and dispatcherID: ' + self.dispatcherID + '. ex: ' + ex)

        #
        # Get the list of dataset resources and add a subscription for the first one
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        
        log.debug('Calling findDataResources to get list of resources to do subscription on.')
        rspMsg = yield self.aisc.findDataResources(reqMsg, ANONYMOUS_USER_ID)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("findDataResources failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].dataResourceSummary)
        if numResReturned == 0:
            self.fail('ERROR: findDataResources returned no resources.')
        else:
            log.debug('findDataResources returned: ' + str(numResReturned) + ' resources.')

        # grab the first resource from the list
        datasetMetadata = rspMsg.message_parameters_reference[0].dataResourceSummary[0].datasetMetadata
        dsResourceID = datasetMetadata.data_resource_id
        
        #
        # Add a subscription for this user to this data resource
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = dsResourceID
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = dsResourceID
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = datasetMetadata.ion_time_coverage_start
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = datasetMetadata.ion_time_coverage_end
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = datasetMetadata.ion_geospatial_lat_min
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = datasetMetadata.ion_geospatial_lat_max
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = datasetMetadata.ion_geospatial_lon_min
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = datasetMetadata.ion_geospatial_lon_max

        #
        # Call AIS to create the subscription
        #
        log.debug('Calling createDataResourceSubscription.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to createDataResourceSubscription')
        else:
            log.debug('POSITIVE rspMsg to createDataResourceSubscription')

        #
        # Now call AIS with no message_parameters_referene: should fail
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        log.debug('Calling findDataResourceSubscriptions with no message_parameters: should fail.')
        rspMsg = yield self.aisc.findDataResourceSubscriptions(reqMsg, self.user_id)
        if rspMsg.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('findDataResourceSubscriptions should have returned error')
        else:
            log.debug('Failure rspMsg to findDataResourceSubscriptions: this is correct')
        
        #
        # Now call AIS to find the subscriptions with no bounds
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_SUBSCRIPTIONS_REQ_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id  = self.user_id
        
        log.debug('Calling findDataResourceSubscriptions.')
        rspMsg = yield self.aisc.findDataResourceSubscriptions(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to findDataResourceSubscriptions')
        else:
            log.debug('POSITIVE rspMsg to findDataResourceSubscriptions')
            
        numSubsReturned = len(rspMsg.message_parameters_reference[0].subscriptionListResults)
        #
        # With no bounds on the find sent (above), the subscription created above
        # should be returned.  If it isn't it's an error, so fail test.
        #
        log.info('findFindDataResourceSubscriptions returned: ' + str(numSubsReturned) + ' subscriptions.')
        if numSubsReturned != 1:
            errString = 'findDataResourcesSubscriptions returned ' + str(numSubsReturned) + ' subscriptions.  Should have been 1'
            self.fail(errString)

        #
        # Now call AIS to find the subscriptions with bounds that should return
        # no dataset 
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_SUBSCRIPTIONS_REQ_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.dataBounds.minLatitude  = -50
        reqMsg.message_parameters_reference.dataBounds.maxLatitude  = -40
        reqMsg.message_parameters_reference.dataBounds.minLongitude = 20
        reqMsg.message_parameters_reference.dataBounds.maxLongitude = 30
        reqMsg.message_parameters_reference.dataBounds.minVertical  = 0
        reqMsg.message_parameters_reference.dataBounds.maxVertical  = 30
        reqMsg.message_parameters_reference.dataBounds.posVertical  = 'down'
        reqMsg.message_parameters_reference.dataBounds.minTime      = '2007-01-1T10:00:00Z'
        reqMsg.message_parameters_reference.dataBounds.maxTime      = '2008-08-1T11:00:00Z'
        
        log.debug('Calling findDataResourceSubscriptions.')
        rspMsg = yield self.aisc.findDataResourceSubscriptions(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to findDataResourceSubscriptions')
        else:
            log.debug('POSITIVE rspMsg to findDataResourceSubscriptions')
            
        numSubsReturned = len(rspMsg.message_parameters_reference[0].subscriptionListResults)

        #
        # There should no subscription returned 
        #
        log.info('findFindDataResourceSubscriptions returned: ' + str(numSubsReturned) + ' subscriptions.')
        if numSubsReturned != 0:
            errString = 'findDataResourcesSubscriptions returned ' + str(numSubsReturned) + ' subscriptions.  Should have been none'
            self.fail(errString)

            
    @defer.inlineCallbacks
    def test_updateDataResourceSubscription(self):
        if self.AnalyzeTiming != None:
            log.warning('test_updateDataResourceSubscription: started at ' + str(self.TimeStamps.StartTime))

        # Create a message client and user
        mc = MessageClient(proc=self.test_sup)
        yield self.createUser()
        if self.AnalyzeTiming != None:
            log.warning('test_updateDataResourceSubscription: created user ' + self.TimeStamp())
        
        #
        # Create dispatchers and associations
        #
        log.info("Creating 2 dispatchers for testing")
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource1')
        log.info('Created Dispatcher1 ID: ' + self.dispatcherRes.ResourceIdentity)
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource2')
        self.dispatcherID = self.dispatcherRes.ResourceIdentity
        log.info('Created Dispatcher2 ID: ' + self.dispatcherID)
        if self.AnalyzeTiming != None:
            log.warning('test_updateDataResourceSubscription: created dispatchers ' + self.TimeStamp())
            
        #
        # Now make an association between the user and this dispatcher
        #
        try:
            log.info('Getting user resource instance')
            try:
                self.userRes = yield self.rc.get_instance(self.user_id)
            except ResourceClientError:
                self.fail('Error getting instance of userID: ' + self.user_id)
            log.info('Got user resource instance: ' + self.userRes.ResourceIdentity)
            association = yield self.ac.create_association(self.userRes, HAS_A_ID, self.dispatcherRes)
            if association not in self.userRes.ResourceAssociationsAsSubject:
                self.fail('Error: subject not in association!')
            if association not in self.dispatcherRes.ResourceAssociationsAsObject:
                self.fail('Error: object not in association')
            
            #
            # Put the association in datastore
            #
            log.debug('Storing association: ' + str(association))
            yield self.rc.put_instance(association)

        except AssociationClientError, ex:
            self.fail('Error creating assocation between userID: ' + self.userID + ' and dispatcherID: ' + self.dispatcherID + '. ex: ' + ex)
        if self.AnalyzeTiming != None:
            log.warning('test_updateDataResourceSubscription: created associations ' + self.TimeStamp())
 
        # first create a subscription to be updated
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id  = 'dataset456'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.DISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter  = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset456'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -55.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -45.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 25.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 35.0
        
        log.debug('Calling createDataResourceSubscription.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to createDataResourceSubscription: '+str(rspMsg.error_str))
        else:
            log.debug('POSITIVE rspMsg to createDataResourceSubscription')
        if self.AnalyzeTiming != None:
            log.warning('test_updateDataResourceSubscription: created subscription ' + self.TimeStamp())
            
        # now update the subscription created above
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id  = 'dataset456'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAIL
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter  = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset456'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -55.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -45.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 25.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 35.0
        
        log.debug('Calling updateDataResourceSubscription.')
        rspMsg = yield self.aisc.updateDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to updateDataResourceSubscription: '+str(rspMsg.error_str))
        else:
            log.debug('POSITIVE rspMsg to updateDataResourceSubscription')
        if self.AnalyzeTiming != None:
            log.warning('test_updateDataResourceSubscription: updated subscription ' + self.TimeStamp())

        # now update the subscription updated above
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id  = 'dataset456'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter  = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset456'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -55.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -45.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 25.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 35.0
        
        log.debug('Calling updateDataResourceSubscription.')
        rspMsg = yield self.aisc.updateDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to updateDataResourceSubscription: '+str(rspMsg.error_str))
        else:
            log.debug('POSITIVE rspMsg to updateDataResourceSubscription')
        if self.AnalyzeTiming != None:
            log.warning('test_updateDataResourceSubscription: updated subscription ' + self.TimeStamp())

    @defer.inlineCallbacks
    def test_deleteDataResourceSubscription(self):
        log.debug('Testing deleteDataResourceSubscriptions.')

        # Create a message client and user
        mc = MessageClient(proc=self.test_sup)
        yield self.createUser()
        
        #
        # Send a request with wrong message type to test that the appropriate error is returned.
        #
        reqMsg = yield mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        log.debug('Calling deleteDataResourceSubscription with wrong message type.')
        rspMsg = yield self.aisc.deleteDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to deleteDataResourceSubscription with wrong message type')
        else:
            self.fail('POSITIVE rspMsg to deleteDataResourceSubscription with wrong message type')
            
        #
        # Send a request without subscriptions to test that the appropriate error is returned.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(DELETE_SUBSCRIPTION_REQ_TYPE)
        log.debug('Calling deleteDataResourceSubscription without subscriptions.')
        rspMsg = yield self.aisc.deleteDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to deleteDataResourceSubscription without subscriptions')
        else:
            self.fail('POSITIVE rspMsg to deleteDataResourceSubscription without subscriptions')
            
        #
        # Send a request without user_ooi_id to test that the appropriate error is returned.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(DELETE_SUBSCRIPTION_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptions.add();
        reqMsg.message_parameters_reference.subscriptions[0].data_src_id  = 'dataset456'
        log.debug('Calling deleteDataResourceSubscription without user_ooi_id.')
        rspMsg = yield self.aisc.deleteDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to deleteDataResourceSubscription without user_ooi_id')
        else:
            self.fail('POSITIVE rspMsg to deleteDataResourceSubscription without user_ooi_id')
            
        #
        # Send a request without data_src_id to test that the appropriate error is returned.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(DELETE_SUBSCRIPTION_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptions.add();
        reqMsg.message_parameters_reference.subscriptions[0].user_ooi_id  = self.user_id
        log.debug('Calling deleteDataResourceSubscription without data_src_id.')
        rspMsg = yield self.aisc.deleteDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to deleteDataResourceSubscription without data_src_id')
        else:
            self.fail('POSITIVE rspMsg to deleteDataResourceSubscription without data_src_id')
            
        #
        # Send a request for non-existant subscription to test that the appropriate error is returned.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(DELETE_SUBSCRIPTION_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptions.add();
        reqMsg.message_parameters_reference.subscriptions[0].user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.subscriptions[0].data_src_id  = 'dataset456'
        log.debug('Calling deleteDataResourceSubscription with non-existant subscription .')
        rspMsg = yield self.aisc.deleteDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.debug('ERROR rspMsg to deleteDataResourceSubscription with for non-existant subscription')
        else:
            self.fail('POSITIVE rspMsg to deleteDataResourceSubscription with for non-existant subscription')
            
        #
        # Create dispatchers and associations
        #
        log.info("Creating 2 dispatchers for testing")
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource1')
        log.info('Created Dispatcher1 ID: ' + self.dispatcherRes.ResourceIdentity)
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource2')
        self.dispatcherID = self.dispatcherRes.ResourceIdentity
        log.info('Created Dispatcher2 ID: ' + self.dispatcherID)
            
        #
        # Now make an association between the user and this dispatcher
        #
        try:
            log.info('Getting user resource instance')
            try:
                self.userRes = yield self.rc.get_instance(self.user_id)
            except ResourceClientError:
                self.fail('Error getting instance of userID: ' + self.user_id)
            log.info('Got user resource instance: ' + self.userRes.ResourceIdentity)
            association = yield self.ac.create_association(self.userRes, HAS_A_ID, self.dispatcherRes)
            if association not in self.userRes.ResourceAssociationsAsSubject:
                self.fail('Error: subject not in association!')
            if association not in self.dispatcherRes.ResourceAssociationsAsObject:
                self.fail('Error: object not in association')
            
            #
            # Put the association in datastore
            #
            log.debug('Storing association: ' + str(association))
            yield self.rc.put_instance(association)

        except AssociationClientError, ex:
            self.fail('Error creating assocation between userID: ' + self.userID + ' and dispatcherID: ' + self.dispatcherID + '. ex: ' + ex)

        #
        # Create a couple of subscriptions to delete later...
        #
        
        #
        # Add a subscription for this user to this data resource
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0

        #
        # Call AIS to create the subscription
        #
        log.debug('Calling createDataResourceSubscription.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to createDataResourceSubscription')
        else:
            log.debug('POSITIVE rspMsg to createDataResourceSubscription')

        #
        # Add another subscription for this user to this data resource
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset456'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset456'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -55.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -45.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 25.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 35.0
    
        #
        # Call AIS to create the subscription
        #
        log.debug('Calling createDataResourceSubscription.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to createDataResourceSubscription')
        else:
            log.debug('POSITIVE rspMsg to createDataResourceSubscription')
           
        # now delete the subscription created above
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(DELETE_SUBSCRIPTION_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptions.add();
        reqMsg.message_parameters_reference.subscriptions[0].user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.subscriptions[0].data_src_id  = 'dataset456'
        reqMsg.message_parameters_reference.subscriptions.add();
        reqMsg.message_parameters_reference.subscriptions[1].user_ooi_id  = self.user_id
        reqMsg.message_parameters_reference.subscriptions[1].data_src_id  = 'dataset123'

        log.debug('Calling deleteDataResourceSubscriptions.')
        rspMsg = yield self.aisc.deleteDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to deleteDataResourceSubscription: '+str(rspMsg.error_str))
        else:
            log.info('POSITIVE rspMsg to deleteDataResourceSubscription')

        # now delete the subscriptions deleted above again
        log.debug('Calling deleteDataResourceSubscriptions a second time.')
        rspMsg = yield self.aisc.deleteDataResourceSubscription(reqMsg, self.user_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            log.info('correct ERROR rspMsg to deleteDataResourceSubscription')
        else:
            self.fail('rspMsg to deleteDataResourceSubscription was not an error')


    @defer.inlineCallbacks
    def test_updateDataResourceCache(self):
        log.debug('Testing updateDataResourceCache.')

        #
        # This test doesn't much other than sending an event to the cache;
        # there should be a more rigorous test that actually makes a modification
        # and sends it to manage data resources, but that test would be an
        # integration test.
        #
        subproc = Process()
        yield subproc.spawn()
        testsub = TestDatasetUpdateEventSubscriber(process=subproc)
        yield testsub.initialize()
        yield testsub.activate()

        #
        # Send a message with no bounds to get a list of dataset ID's; then
        # take one of those IDs and publish a SupplementAdded event for it
        #
        
        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create a request message 
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id  = ANONYMOUS_USER_ID
        rspMsg = yield self.aisc.findDataResources(reqMsg, reqMsg.message_parameters_reference.user_ooi_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail("test_updateDataResourceCache failed: " + rspMsg.error_str)

        numResReturned = len(rspMsg.message_parameters_reference[0].dataResourceSummary)
        log.debug('test_updateDataResourceCache returned: ' + str(numResReturned) + ' resources.')

        self.__validateDataResourceSummary(rspMsg.message_parameters_reference[0].dataResourceSummary)

        if numResReturned > 0:
            log.debug('test_updateDataResourceCache: %s datasets returned!' % (numResReturned))
            dsID = rspMsg.message_parameters_reference[0].dataResourceSummary[0].datasetMetadata.data_resource_id

            # Setup the publisher
            pub1 = DatasetChangeEventPublisher(process=self._proc)
            yield pub1.initialize()
            yield pub1.activate()
            
            yield pu.asleep(1.0)
            
            yield pub1.create_and_publish_event(
                name = "TestUpdateDataResourceCache",
                origin = "SOME DATASET RESOURCE ID",
                dataset_id = dsID,
                )
    
            # Pause to make sure we catch the message
            yield pu.asleep(3.0)
            
            #self.assertEqual(testsub.msgs[0]['content'].name, u"TestUpdateEvent")

        else:
            log.error('test_updateDataResourceCache: No datasets returned!')
        

    def __validateDatasetByOwnerMetadata(self, metadata):
        log.debug('__validateDataResourceSummary()')
        
        i = 0
        while i < len(metadata):
            data = metadata[i]
            if not data.IsFieldSet('data_resource_id'):
                self.fail('FindDataResourcesByOwner response has no data_resource_id field')
            else:                
                data.data_resource_id
            if not data.IsFieldSet('update_interval_seconds'):
                self.fail('FindDataResourcesByOwner response has no update_interval_seconds field')
            else:                
                log.debug('update_interval_seconds: ' + str(data.update_interval_seconds))
            if not data.IsFieldSet('ion_title'):
                self.fail('FindDataResourcesByOwner response has no ion_title field')
            else:                
                log.debug('ion_title: ' + str(data.update_interval_seconds))
            if not data.IsFieldSet('date_registered'):
                self.fail('FindDataResourcesByOwner response has no date_registered field')
            else:                
                log.debug('date_registered: ' + str(data.date_registered))
            if not data.IsFieldSet('title'):
                self.fail('FindDataResourcesByOwner response has no title field')
            else:                
                log.debug('title: ' + str(data.title))
            if not data.IsFieldSet('activation_state'):
                self.fail('FindDataResourcesByOwner response has no activation_state field')
            else:                
                log.debug('activation_state: ' + str(data.activation_state))
            i = i + 1                

    def __validateNotificationSet(self, dataResourceSummaries, dsID):
        log.debug('__validateNotificationSet()')
        
        i = 0
        while i < len(dataResourceSummaries):
            datasetMetadata = dataResourceSummaries[i].datasetMetadata
            if dsID == datasetMetadata.data_resource_id:
                if dataResourceSummaries[i].IsFieldSet('notificationSet'):
                    if not dataResourceSummaries[i].notificationSet == True:
                        self.fail('dataset: ' +  dsID + \
                            ' notificationSet should be True, but is False')
            i = i + 1                        


    def __validateDataResourceSummary(self, dataResourceSummaries):
        log.debug('__validateDataResourceSummary()')
        
        i = 0
        while i < len(dataResourceSummaries):
            datasetMetadata = dataResourceSummaries[i].datasetMetadata
            dsResourceID = datasetMetadata.data_resource_id

            if not dataResourceSummaries[i].IsFieldSet('notificationSet'):
                self.fail('dataset: ' +  dsResourceID + ' has no notificationSet field')
            if not dataResourceSummaries[i].IsFieldSet('date_registered'):
                self.fail('dataset: ' +  dsResourceID + ' has no date_registered field')
            log.info('date registered: ' + str(dataResourceSummaries[i].date_registered))

            #if not datasetMetadata.IsFieldSet('user_ooi_id'):
            #    self.fail('dataset: ' +  dsResourceID + ' has no user_ooi_id field')
            if not datasetMetadata.IsFieldSet('data_resource_id'):
                self.fail('dataset: ' +  dsResourceID + ' has no resource_id field')
            if not datasetMetadata.IsFieldSet('title'):
                #self.fail('response to findDataResources has no title field')
                log.error('dataset: ' +  dsResourceID + ' has no title field')
            if not datasetMetadata.IsFieldSet('institution'):
                #self.fail('response to findDataResources has no institution field')
                log.error('dataset: ' +  dsResourceID + ' has no institution field')
            if not datasetMetadata.IsFieldSet('source'):
                #self.fail('response to findDataResources has no source field')
                log.error('dataset: ' +  dsResourceID + ' has no source field')
            if not datasetMetadata.IsFieldSet('references'):
                #self.fail('response to findDataResources has no references field')
                log.error('dataset: ' +  dsResourceID + ' has no references field')
            if not datasetMetadata.IsFieldSet('ion_time_coverage_start'):
                self.fail('dataset: ' +  dsResourceID + ' has no ion_time_coverage_start field')
            if not datasetMetadata.IsFieldSet('ion_time_coverage_end'):
                self.fail('dataset: ' +  dsResourceID + ' has no ion_time_coverage_end field')
            if not datasetMetadata.IsFieldSet('summary'):
                #self.fail('response to findDataResources has no summary field')
                log.error('dataset: ' +  dsResourceID + ' has no summary field')
            if not datasetMetadata.IsFieldSet('comment'):
                #self.fail('response to findDataResources has no comment field')
                log.error('dataset: ' +  dsResourceID + ' has no comment field')
            if not datasetMetadata.IsFieldSet('ion_geospatial_lat_min'):
                self.fail('dataset: ' +  dsResourceID + ' has no ion_geospatial_lat_min field')
            if not datasetMetadata.IsFieldSet('ion_geospatial_lat_max'):
                self.fail('dataset: ' +  dsResourceID + ' has no ion_geospatial_lat_max field')
            if not datasetMetadata.IsFieldSet('ion_geospatial_lon_min'):
                self.fail('dataset: ' +  dsResourceID + ' has no ion_geospatial_lon_min field')
            if not datasetMetadata.IsFieldSet('ion_geospatial_lon_max'):
                self.fail('dataset: ' +  dsResourceID + ' no ion_geospatial_lon_max field')
            if not datasetMetadata.IsFieldSet('ion_geospatial_vertical_min'):
                self.fail('dataset: ' +  dsResourceID + ' has no ion_geospatial_vertical_min field')
            if not datasetMetadata.IsFieldSet('ion_geospatial_vertical_max'):
                self.fail('dataset: ' +  dsResourceID + ' has no ion_geospatial_vertical_max field')
            if not datasetMetadata.IsFieldSet('ion_geospatial_vertical_positive'):
                self.fail('dataset: ' +  dsResourceID + ' has no ion_geospatial_vertical_positive field')
            if not datasetMetadata.IsFieldSet('download_url'):
                self.fail('dataset: ' +  dsResourceID + ' has no download_url field')
            i = i + 1                


    def __printMetadata(self, rspMsg):

        #
        # If there are any dataset "other_attributes," print them.
        #
        if len(rspMsg.message_parameters_reference[0].other_attributes) > 0:
            log.debug('Dataset Other Attributes:')
            for attr in rspMsg.message_parameters_reference[0].other_attributes:
                log.debug('  attribute name: %s' %(attr.name))
                log.debug('  attribute value: %s' %(attr.value))

        #
        # If there are any dataset dimensions, print them.
        #
        if len(rspMsg.message_parameters_reference[0].dimensions) > 0:
            log.debug('Dataset Dimensions:')
            for dim in rspMsg.message_parameters_reference[0].dimensions:
                log.debug('  dimension name: %s' %(dim.name))
                log.debug('  dimension length: %d' %(dim.length))

        #
        # Print any variables.  Variables can have their own "other_attributes"
        # and dimensions, so print those too.
        #
        log.debug('Dataset Variables:')
        for var in rspMsg.message_parameters_reference[0].variable:
            log.debug('  Variable name: %s' %(var.name))
            log.debug('    standard_name: ' + var.standard_name)
            log.debug('    long_name: ' + var.long_name)
            log.debug('    units: ' + var.units)
            
            if len(var.other_attributes) > 0:
                log.debug('    Other Attributes for Variable %s:' %(var.name))
                for attrib in var.other_attributes:
                    log.debug('      name: %s, value: %s' %(attrib.name, attrib.value))

            if len(var.dimensions) > 0:
                log.debug('    Dimensions for Variable %s:' %var.name)
                for dim in var.dimensions:
                    log.debug('      name: %s, length: %s' %(dim.name, dim.length))



    @defer.inlineCallbacks
    def createUser(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_REQUEST_TYPE)

        # fill in the certificate and key for
        # subject = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"

        msg.message_parameters_reference.certificate = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        msg.message_parameters_reference.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtbg0kKLmivgoVsA4U7swNDRH6svW24
2THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq7LWt2T6GVVA10ex5WAeB/o7br/Z4
U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b2lUtQc6cjuHRDU4NknXaVMXTBHKP
M40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4dszsqn2SC8YDw1xrujvW2Bd7Q7Bw
MQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+6M6SMQIDAQABAoIBAAc/Ic97ZDQ9
tFh76wzVWj4SVRuxj7HWSNQ+Uzi6PKr8Zy182Sxp74+TuN9zKAppCQ8LEKwpkKtEjXsl8QcXn38m
sXOo8+F1He6FaoRQ1vXi3M1boPpefWLtyZ6rkeJw6VP3MVG5gmho0VaOqLieWKLP6fXgZGUhBvFm
yxUPoNgXJPLjJ9pNGy4IBuQDudqfJeqnbIe0GOXdB1oLCjAgZlTR4lFA92OrkMEldyVp72iYbffN
4GqoCEiHi8lX9m2kvwiQKRnfH1dLnnPBrrwatu7TxOs02HpJ99wfzKRy4B1SKcB0Gs22761r+N/M
oO966VxlkKYTN+soN5ID9mQmXJkCgYEA/h2bqH9mNzHhzS21x8mC6n+MTyYYKVlEW4VSJ3TyMKlR
gAjhxY/LUNeVpfxm2fY8tvQecWaW3mYQLfnvM7f1FeNJwEwIkS/yaeNmcRC6HK/hHeE87+fNVW/U
ftU4FW5Krg3QIYxcTL2vL3JU4Auu3E/XVcx0iqYMGZMEEDOcQPcCgYEA6sLLIeOdngUvxdA4KKEe
qInDpa/coWbtAlGJv8NueYTuD3BYJG5KoWFY4TVfjQsBgdxNxHzxb5l9PrFLm9mRn3iiR/2EpQke
qJzs87K0A/sxTVES29w1PKinkBkdu8pNk10TxtRUl/Ox3fuuZPvyt9hi5c5O/MCKJbjmyJHuJBcC
gYBiAJM2oaOPJ9q4oadYnLuzqms3Xy60S6wUS8+KTgzVfYdkBIjmA3XbALnDIRudddymhnFzNKh8
rwoQYTLCVHDd9yFLW0d2jvJDqiKo+lV8mMwOFP7GWzSSfaWLILoXcci1ZbheJ9607faxKrvXCEpw
xw36FfbgPfeuqUdI5E6fswKBgFIxCu99gnSNulEWemL3LgWx3fbHYIZ9w6MZKxIheS9AdByhp6px
lt1zeKu4hRCbdtaha/TMDbeV1Hy7lA4nmU1s7dwojWU+kSZVcrxLp6zxKCy6otCpA1aOccQIlxll
Vc2vO7pUIp3kqzRd5ovijfMB5nYwygTB4FwepWY5eVfXAoGBAIqrLKhRzdpGL0Vp2jwtJJiMShKm
WJ1c7fBskgAVk8jJzbEgMxuVeurioYqj0Cn7hFQoLc+npdU5byRti+4xjZBXSmmjo4Y7ttXGvBrf
c2bPOQRAYZyD2o+/MHBDsz7RWZJoZiI+SJJuE4wphGUsEbI2Ger1QW9135jKp6BsY2qZ
-----END RSA PRIVATE KEY-----"""

        # try to register this user for the first time
        reply = yield self.aisc.registerUser(msg, ANONYMOUS_USER_ID)
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != REGISTER_USER_RESPONSE_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        if reply.message_parameters_reference[0].user_is_early_adopter != True:
            self.fail("response does not indicate user is an early adopter")
        self.user_id = reply.message_parameters_reference[0].ooi_id
        log.info("AppIntegrationTest:createUser id = "+str(self.user_id))


        # Give our test user an email address
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS updateUserProfile request')
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_PROFILE_REQUEST_TYPE)
        msg.message_parameters_reference.user_ooi_id = self.user_id
        msg.message_parameters_reference.email_address = "mmmanning@ucsd.edu"
        msg.message_parameters_reference.profile.add()
        msg.message_parameters_reference.profile[0].name = "profile item 1 name"
        msg.message_parameters_reference.profile[0].value = "profile item 1 value"
        msg.message_parameters_reference.profile.add()
        msg.message_parameters_reference.profile[1].name = "profile item 2 name"
        msg.message_parameters_reference.profile[1].value = "profile item 2 value"
        try:
            reply = yield self.aisc.updateUserProfile(msg, msg.message_parameters_reference.user_ooi_id)
        except ReceivedApplicationError:
            self.fail('updateUserProfile incorrectly raised exception for an authenticated ooi_id')
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('updateUserProfile returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')


    @defer.inlineCallbacks
    def __createSubscriptions(self, datasetID, user_id):
        log.debug('__createSubscriptions().')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        #
        # Create dispatchers and associations
        #
        log.info("Creating 2 dispatchers for testing")
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource1')
        log.info('Created Dispatcher1 ID: ' + self.dispatcherRes.ResourceIdentity)
        self.dispatcherRes = yield self.__register_dispatcher('DispatcherResource2')
        self.dispatcherID = self.dispatcherRes.ResourceIdentity
        log.info('Created Dispatcher2 ID: ' + self.dispatcherID)
            
        #
        # Now make an association between the user and this dispatcher
        #
        try:
            log.info('Getting user resource instance')
            try:
                self.userRes = yield self.rc.get_instance(user_id)
            except ResourceClientError:
                self.fail('Error getting instance of userID: ' + user_id)
            log.info('Got user resource instance: ' + self.userRes.ResourceIdentity)
            association = yield self.ac.create_association(self.userRes, HAS_A_ID, self.dispatcherRes)
            if association not in self.userRes.ResourceAssociationsAsSubject:
                self.fail('Error: subject not in association!')
            if association not in self.dispatcherRes.ResourceAssociationsAsObject:
                self.fail('Error: object not in association')
            
            #
            # Put the association in datastore
            #
            log.debug('Storing association: ' + str(association))
            yield self.rc.put_instance(association)

        except AssociationClientError, ex:
            self.fail('Error creating assocation between userID: ' + self.userID + ' and dispatcherID: ' + self.dispatcherID + '. ex: ' + ex)

        #
        # Add a couple of subscriptions to find later...
        #
        
        #
        # Add a subscription for this user to this data resource
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = datasetID
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = datasetID
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0

        #
        # Call AIS to create the subscription
        #
        log.debug('Calling createDataResourceSubscription.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to createDataResourceSubscription')
        else:
            log.debug('POSITIVE rspMsg to createDataResourceSubscription')

        #
        # Add another subscription for this user to this data resource
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset456'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = user_id
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset456'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -55.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -45.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 25.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 35.0
    
        #
        # Call AIS to create the subscription
        #
        log.debug('Calling createDataResourceSubscription.')
        rspMsg = yield self.aisc.createDataResourceSubscription(reqMsg, reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id)
        if rspMsg.MessageType == AIS_RESPONSE_ERROR_TYPE:
            self.fail('ERROR rspMsg to createDataResourceSubscription')
        else:
            log.debug('POSITIVE rspMsg to createDataResourceSubscription')

