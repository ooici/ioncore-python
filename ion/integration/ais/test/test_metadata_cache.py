#!/usr/bin/env python

"""
@file ion/integration/test_app_integration.py
@test ion.integration.app_integration_service
@author David Everett
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import ion.util.procutils as pu

from twisted.internet import defer
import time
    
from ion.core.process.process import Process
from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.core.data.storage_configuration_utility import COMMIT_CACHE

from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceClientError
from ion.services.coi.resource_registry.association_client import AssociationClient
from ion.services.dm.distribution.events import DatasetChangeEventPublisher, \
                                                DatasourceChangeEventPublisher
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG, ION_AIS_RESOURCES_CFG

from ion.test.iontest import IonTestCase

from ion.integration.ais.findDataResources.findDataResources import DatasetUpdateEventSubscriber, \
                                                                    DatasourceUpdateEventSubscriber
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
            ]

        log.debug('MetadataCacheTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('MetadataCacheTest.setUp(): spawned processes')

        self.sup = sup

        self.publisher_proc = Process(**{'proc-name':'Test Publisher Proc'})

        #
        # Instantiate the caching object
        #
        subproc = Process(**{'proc-name':'Test Metadata Cache Subscriber Proc'})
        yield subproc.spawn()

        self.cache = MetadataCache(subproc)
        log.debug('Instantiated AIS Metadata Cache Object')
        subproc.metadataCache = self.cache
        subproc.rc = ResourceClient(subproc)

        yield self.cache.loadDataSets()
        yield self.cache.loadDataSources()

        self.subproc = subproc

        if self.AnalyzeTiming != None:
            self.TimeStamps.StartTime = time.time()
            self.TimeStamps.LastTime = self.TimeStamps.StartTime


        # Setup the publishers
        datasetPublisher = DatasetChangeEventPublisher(process=self.publisher_proc)
        yield datasetPublisher.initialize()
        yield datasetPublisher.activate()
        self.datasetPublisher = datasetPublisher

        datasrcPublisher = DatasourceChangeEventPublisher(process=self.publisher_proc)
        yield datasrcPublisher.initialize()
        yield datasrcPublisher.activate()
        self.datasrcPublisher = datasrcPublisher

        # Setup the subscribers
        log.info('instantiating DatasetUpdateEventSubscriber')
        self.dataset_subscriber = DatasetUpdateEventSubscriber(process = self.subproc)
        yield self.dataset_subscriber.initialize()
        yield self.dataset_subscriber.activate()

        log.info('instantiating DatasourceUpdateEventSubscriber')
        self.datasource_subscriber = DatasourceUpdateEventSubscriber(process = self.subproc)
        yield self.datasource_subscriber.initialize()
        yield self.datasource_subscriber.activate()



    @defer.inlineCallbacks
    def tearDown(self):
        log.info('Tearing Down Test Container')


        yield self._shutdown_processes()
        yield self._stop_container()

    def getMetadataCache(self):
        return self.cache

    @defer.inlineCallbacks
    def test_metadataCache(self):
        log.debug('Testing updateMetadataCache.')

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
            # Dataset metadata should exist since we got it from the cache list.
            # If it doesn't, fail test.
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

        dsourcelist = self.cache.getDataSources()
        for ds in dsourcelist:
            dSourceResID = ds['dsource'].ResourceIdentity

            dSourceMetadata = yield self.cache.getDSourceMetadata(dSourceResID)

            if dSourceMetadata is None:
                self.fail("test_metadataCache failed: dSourceMetadata returned None for dsource %s" %(dSourceResID))
            else:
                #
                # Delete the dataset
                #
                log.debug('DataSourceUpdatetest deleting %s' \
                          %(dSourceResID))
                yield self.cache.deleteDSourceMetadata(dSourceResID)

            #
            # Now  reload the dataset and datasource metadata
            #
            log.debug('DatasetUpdate Test putting new metadata in cache for %s' %(dSourceResID))
            yield self.cache.putDSourceMetadata(dSourceResID)

        self.assertEqual(numDatasets, self.cache.numDSets)
        self.assertEqual(numDatasources, self.cache.numDSources)


    @defer.inlineCallbacks
    def test_updateMetadataCache(self):
        log.debug('Testing updateMetadataCache.')

        numDatasets = self.cache.getNumDatasets()
        numDatasources = self.cache.getNumDatasources()
           
        dsList = self.cache.getDatasets()
        log.debug('List of datasets returned from metadataCache:')
        for ds in dsList:
            dSetID = ds['ResourceIdentity']
            dSrcID = ds['DSourceID']
            
            log.debug('publishing event for dSetID: %s' %(dSetID))

            source_hook = self.datasource_subscriber._hook_for_testing
            set_hook = self.dataset_subscriber._hook_for_testing

            yield self.datasetPublisher.create_and_publish_event(
                name = "TestUpdateDataResourceCache",
                origin = "SOME DATASET RESOURCE ID",
                dataset_id = dSetID,
                )
            
            log.debug('publishing event for dSrcID: %s' %(dSrcID))

            yield self.datasrcPublisher.create_and_publish_event(
                name = "TestUpdateDataResourceCache",
                origin = "SOME DATASOURCE RESOURCE ID",
                datasource_id = dSrcID,
                )

            # Hook will call back when the event is processed in ondata
            yield source_hook
            yield set_hook

            # Must reset the deferred Manually
            self.datasource_subscriber._hook_for_testing = defer.Deferred()
            self.dataset_subscriber._hook_for_testing = defer.Deferred()

        self.assertEqual(numDatasets, self.cache.numDSets)
        self.assertEqual(numDatasources, self.cache.numDSources)
    
        # Pause to make sure we catch the message
        log.debug('TestUpdateDataResourceCache shutting down...')
            

    @defer.inlineCallbacks
    def test_refreshDatasets(self):
        log.debug('Testing refreshDataset.')
        
        #
        # The purpose of this test is to test the case where a datasource
        # is in the inactive state, and therefore the associated dataset
        # is not loaded into the cache and won't show up in the list of
        # datasets.  However, when the datasource gets set to actve and
        # the datasource change event is sent, the associated dataset(s)
        # should be refreshed and get into the list since their source is
        # active.
        #

        #
        # Set all ACTIVE datasources to inactive
        #
        dSourceIDChangedList = []
        dSetIDOrphanList = []
        dsList = self.cache.getDatasets()
        origNumDSets = len(dsList)
        log.debug('List of (%d) datasets returned from metadataCache: %s' %(origNumDSets, dsList))

        # Create a new process and separate resource client to modify the resource
        rc = ResourceClient()

        for ds in dsList:
            #dSetID = ds['ResourceIdentity']
            dSourceID = ds['DSourceID']
            
            try:
                dSource = yield rc.get_instance(dSourceID)
            except ResourceClientError:    
                log.error('get_instance failed for data source ID %s !' %(dSourceID))
            else:
                #
                # if the lifecycle state is ACTIVE, set it to RETIRED
                #
                if dSource.ResourceLifeCycleState == dSource.ACTIVE:
                    dSourceIDChangedList.append(dSourceID)
                    newOrphanIDList = yield self.cache.getAssociatedDatasets(dSource)
                    for newDSetID in newOrphanIDList:
                        dSetIDOrphanList.append(newDSetID)
                    log.debug('dSetIDOrphanList = %s' %(dSetIDOrphanList))
                    log.debug("Setting data source %s lifecycle = retired" %(dSourceID))
                    dSource.ResourceLifeCycleState = dSource.RETIRED
                    yield rc.put_instance(dSource)

                log.debug('publishing event for dSourceID: %s' %(dSourceID))
    
                #
                # publish the event so the datasource and dataset cache are
                # refreshed
                #

                source_hook = self.datasource_subscriber._hook_for_testing
                yield self.datasrcPublisher.create_and_publish_event(
                    name = "TestUpdateDataResourceCache",
                    origin = "SOME DATASOURCE RESOURCE ID",
                    datasource_id = dSourceID,
                    )

                # Hook will call back when the event is processed in ondata
                yield source_hook

                # Must reset the deferred Manually
                self.datasource_subscriber._hook_for_testing = defer.Deferred()

        #
        # Check how many datasets are in the list
        #
        dsList = self.cache.getDatasets()
        log.debug('List of (%d) datasets returned from metadataCache: %s' %(len(dsList), dsList))
        
        #
        # Now simulate what findDataResources would do: getting the list of datasets
        # (assuming they are in bounds).  At the end of this for loop, there should
        # be no datasets in the count, because their datasources are inactive.
        #
        for dSetID in dSetIDOrphanList:
            dSet = yield self.cache.getDSetMetadata(dSetID)
            if dSet is None:
                log.info('dSet for dSetID %s is None' % (dSetID))
                continue
            
            dSourceID = dSet['DSourceID']
            self.failIfIdentical(dSourceID, None )

            dSource = yield self.cache.getDSourceMetadata(dSourceID)

            self.failUnlessIdentical(dSource, None, 'There should be no Data Source metadata')

        #
        # Now set all datasources that were changed back to ACTIVE
        #
        for dSourceID in dSourceIDChangedList:
            try:
                dSource = yield rc.get_instance(dSourceID)
            except ResourceClientError:    
                log.error('get_instance failed for data source ID %s !' %(dSourceID))
            else:
                #
                # set the lifecycle state to ACTIVE
                #
                log.debug("Setting data source %s lifecycle to ACTIVE" %(dSourceID))
                dSource.ResourceLifeCycleState = dSource.ACTIVE
                yield rc.put_instance(dSource)

                log.debug('publishing event for dSourceID: %s' %(dSourceID))
    
                #
                # publish the event so the datasource and dataset cache are
                # refreshed
                #
                src_hook = self.datasource_subscriber._hook_for_testing

                yield self.datasrcPublisher.create_and_publish_event(
                    name = "TestUpdateDataResourceCache",
                    origin = "SOME DATASOURCE RESOURCE ID",
                    datasource_id = dSourceID,
                    )

                yield src_hook
                # Must reset the deferred Manually
                self.datasource_subscriber._hook_for_testing = defer.Deferred()
                
        #
        # There should now be origNumDSets datasets in the list
        #
        dsList = self.cache.getDatasets()
        log.debug('List of (%d) datasets returned from metadataCache: %s' %(len(dsList), dsList))

        #
        # Now get the datasets that would show up in the list (assuming they are in bounds)
        #
        for dSet in dsList:
            dSourceID = dSet['DSourceID']
            self.failIfIdentical(dSourceID, None )


            dSource = yield self.cache.getDSourceMetadata(dSourceID)
            self.failIfIdentical(dSource, None, 'There should be Data Source metadata')

            