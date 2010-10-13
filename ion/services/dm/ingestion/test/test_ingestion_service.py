#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/test/test_ingestion_service.py
@author Paul Hubbard
@date 6/11/10
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.services.dm.ingestion.ingestion_service import IngestionClient

from ion.resources.dm_resource_descriptions import IngestionStreamResource


class IngesterTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 30
        services = [
            {'name': 'ingestion_registry',
             'module': 'ion.services.dm.ingestion.ingestion_registry',
             'class':'IngestionRegistryService'},
            {'name': 'ingestion_service',
             'module': 'ion.services.dm.ingestion.ingestion_service',
             'class':'IngestionService'},
            {'name': 'preservation_registry',
             'module': 'ion.services.dm.preservation.preservation_registry',
             'class':'PreservationRegistryService'},
            {'name': 'preservation_service',
             'module': 'ion.services.dm.preservation.preservation_service',
             'class':'PreservationService'},
            {'name': 'data_registry',
             'module': 'ion.services.dm.inventory.data_registry',
             'class':'DataRegistryService'},
            {'name': 'datapubsub_registry',
             'module': 'ion.services.dm.distribution.pubsub_registry',
             'class':'DataPubsubRegistryService'},
            {'name': 'data_pubsub',
             'module': 'ion.services.dm.distribution.pubsub_service',
             'class':'DataPubsubService'}
        ]
        sup = yield self._spawn_processes(services)
        self.ic = IngestionClient(proc=sup)
        #self.fs = FetcherService()
        #self.mys = IngestService()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_instantiation_only(self):
        pass

    def test_create_ingestion_datastream(self):
        """
        Use fetcher service to create dataset so we can test without
        actual messaging.
        """
        isr = IngestionStreamResource.create_new_resource()
        isr.name = 'sbe49_id_28461984'

        # isr = yield self.ic.create_ingestion_datastream(isr)
        # Will return a complete ingestion stream resource with topics for input and ingested data

        raise unittest.SkipTest('Not implemented')
