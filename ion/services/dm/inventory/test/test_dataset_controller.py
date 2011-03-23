#!/usr/bin/env python

"""
@file ion/play/test/test_hello_resource.py
@test ion.play.hello_resource Example unit tests for sample resource code.
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.play.hello_resource import HelloResourceClient
from ion.test.iontest import IonTestCase

from ion.core.messaging.message_client import MessageClient

from ion.services.dm.inventory.dataset_controller import QUERYRESULTS_TYPE, FINDDATASETREQUEST_TYPE, IDREF_TYPE, DatasetControllerClient

class DateSetControllerTest(IonTestCase):
    """
    Testing example hello resource service.
    This example shows how it is possible to create and send resource requests.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {'name':'dataset_controller','module':'ion.services.dm.inventory.dataset_controller','class':'DataSetController'},
        ]

        sup = yield self._spawn_processes(services)

        self.mc = MessageClient(proc = self.test_sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_hello_dataset(self):

        # Create a hello object client
        dscc = DatasetControllerClient(proc=self.test_sup)

        # Creating a new dataset is takes input - it is creating blank resource to be filled by ingestion
        create_request_msg = yield self.mc.create_instance(None)

        # You can send the root of the object or any linked composite part of it.
        create_response_msg = yield dscc.create_dataset_resource(create_request_msg)

        log.info('Create returned resource reference:\n%s' % str(create_response_msg))
        

    @defer.inlineCallbacks
    def test_hello_dataset(self):

        # Create a hello object client
        dscc = DatasetControllerClient(proc=self.test_sup)

        # Creating a new dataset is takes input - it is creating blank resource to be filled by ingestion
        create_request_msg = yield self.mc.create_instance(None)

        # You can send the root of the object or any linked composite part of it.
        create_response_msg = yield dscc.create_dataset_resource(create_request_msg)

        log.info('Create returned resource reference:\n%s' % str(create_response_msg))

