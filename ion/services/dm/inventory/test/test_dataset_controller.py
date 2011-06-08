#!/usr/bin/env python

"""
@file ion/play/test/test_hello_resource.py
@test ion.play.hello_resource Example unit tests for sample resource code.
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process import process
from ion.test.iontest import IonTestCase
from ion.services.coi.resource_registry import resource_client
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG
from ion.util.procutils import asleep


# Message types
from ion.services.dm.inventory.dataset_controller import FINDDATASETREQUEST_TYPE, \
    DatasetControllerClient, CMD_DATASET_RESOURCE_TYPE


from ion.core import ioninit
CONF = ioninit.config(__name__)


class DatasetControllerTest(IonTestCase):
    """
    Testing example hello resource service.
    This example shows how it is possible to create and send resource requests.
    """

    #noinspection PyUnusedLocal
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [

            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True},}
                },

            {'name':'association_service',
             'module':'ion.services.dm.inventory.association_service',
             'class':'AssociationService'
              },

            {'name':'resource_registry1','module':'ion.services.coi.resource_registry.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},

            {'name': 'scheduler', 'module': 'ion.services.dm.scheduler.scheduler_service',
             'class': 'SchedulerService'},

            {'name':'dataset_controller',
             'module':'ion.services.dm.inventory.dataset_controller',
             'class':'DataSetController',
             'spawnargs': {'do-init' : False}},
        ]


        sup = yield self._spawn_processes(services)
        self.sup = sup
        # Creat an anonymous process for the tests
        self.proc = process.Process()
        yield self.proc.spawn()

        self.mc = MessageClient(proc=self.proc)
        self.rc = resource_client.ResourceClient(proc=self.proc)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()





    @defer.inlineCallbacks
    def test_hello_dataset(self):

        # Create a Dataset Controller client with an anonymous process
        dscc = DatasetControllerClient(proc=self.proc)

        # Creating a new dataset is takes input - it is creating blank resource to be filled by ingestion
        create_request_msg = yield self.mc.create_instance(None)

        # You can send the root of the object or any linked composite part of it.
        create_response_msg = yield dscc.create_dataset_resource(create_request_msg)

        log.info('Create returned resource reference:\n%s' % str(create_response_msg))
        
        yield asleep(10.0)

    @defer.inlineCallbacks
    def test_find_dataset(self):

        # Create a Dataset Controller client with an anonymous process
        dscc = DatasetControllerClient(proc=self.proc)

        # Creating a new dataset is takes input - it is creating blank resource to be filled by ingestion
        find_request_msg = yield self.mc.create_instance(FINDDATASETREQUEST_TYPE)

        find_request_msg.only_mine = False
        find_request_msg.by_life_cycle_state = find_request_msg.LifeCycleState.ACTIVE

        # You can send the root of the object or any linked composite part of it.
        find_response_msg = yield dscc.find_dataset_resources(find_request_msg)

        log.info('Create returned resource reference:\n%s' % str(find_response_msg))

        # This may fail if more datasets are preloaded

        self.assertEqual(len(find_response_msg.idrefs)>=1,True)

        for idref in find_response_msg.idrefs:

            dataset = yield self.rc.get_instance(idref)

            # Now you have got the dataset object!

            self.assertEqual(dataset.ResourceObjectType, CMD_DATASET_RESOURCE_TYPE)

            self.assertEqual(dataset.ResourceLifeCycleState, dataset.ACTIVE)


    @defer.inlineCallbacks
    def test_find_dataset_by_owner(self):

        # Create a Dataset Controller client with an anonymous process
        dscc = DatasetControllerClient(proc=self.proc)

        # Creating a new dataset is takes input - it is creating blank resource to be filled by ingestion
        find_request_msg = yield self.mc.create_instance(FINDDATASETREQUEST_TYPE)

        find_request_msg.only_mine = True
        # This will default the the anonymous user who owns the default datasets!
        
        find_request_msg.by_life_cycle_state = find_request_msg.LifeCycleState.ACTIVE

        # You can send the root of the object or any linked composite part of it.
        find_response_msg = yield dscc.find_dataset_resources(find_request_msg)

        log.info('Create returned resource reference:\n%s' % str(find_response_msg))

        # This may fail if more datasets are preloaded

        self.assertEqual(len(find_response_msg.idrefs)>=1,True)

        for idref in find_response_msg.idrefs:

            dataset = yield self.rc.get_instance(idref)

            # Now you have got the dataset object!

            self.assertEqual(dataset.ResourceObjectType, CMD_DATASET_RESOURCE_TYPE)

            self.assertEqual(dataset.ResourceLifeCycleState, dataset.ACTIVE)

