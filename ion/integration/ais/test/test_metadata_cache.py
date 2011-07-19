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

from ion.integration.ais.common.metadata_cache import  MetadataCache

#
# ResourceID for testing create download URL response
#
TEST_RESOURCE_ID = '01234567-8abc-def0-1234-567890123456'
DISPATCHER_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=7002, version=1)


class MetadataCacheTest(IonTestCase):
   
    """
    Testing Metadata Cache.
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
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },
            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },
            {
                'name':'attributestore',
                'module':'ion.services.coi.attributestore',
                'class':'AttributeStoreService'
            },
            {
                'name':'store_service',
                'module':'ion.core.data.store_service',
                'class':'StoreService'
            },
            {
                'name':'dataset_controller',
                'module':'ion.services.dm.inventory.dataset_controller',
                'class':'DatasetControllerClient'
            },

            ]

        log.debug('MetadataCacheTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('MetadataCacheTest.setUp(): spawned processes')

        self.sup = sup

        self.rc = ResourceClient(proc=sup)
        self.mc = MessageClient(proc=sup)
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
    def test_metadataCache(self):

        self.cache = MetadataCache(self.sup)
        log.debug('Instantiated AIS Metadata Cache Object')
        yield self.cache.loadDataSets()
        yield self.cache.loadDataSources()
        
        #
        # Log the number of datasets
        #
        numDatasets = self.cache.getNumDatasets()
        numDatasources = self.cache.getNumDatasources()
        log.debug("There are %d datasets and %d datasources" %(numDatasets, numDatasources))

        dsList = self.cache.getDatasets()
        log.debug('List of datasets returned from metadataCache:')
        for ds in dsList:
            dSetResID = ds['ResourceIdentity']
            log.debug('dsID: %s' %(dSetResID))

            #
            # Check the cache to see if there's currently metadata for this
            # datasetID
            #
            dSetMetadata = yield self.cache.getDSetMetadata(dSetResID)
    
            #
            # Dataset should exist since we got it from the cache list
            #
            if dSetMetadata is None:
                self.fail("test_metadataCache failed: dSetMetadata returned None for dataset %s" %(dSetResID))
            else:
                #
                # Delete the dataset
                #
                log.debug('DatasetUpdateEventSubscriber deleting %s' \
                          %(dSetResID))
                yield self.cache.deleteDSetMetadata(dSetResID)

            #
            # Now  reload the dataset and datasource metadata
            #
            log.debug('DatasetUpdateEventSubscriber putting new metadata in cache for %s' %(dSetResID))
            yield self.cache.putDSetMetadata(dSetResID)
