#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/test/test_ingester.py
@author Paul Hubbard
@date 6/11/10
"""

import logging
logging = logging.getLogger(__name__)

from twisted.trial import unittest

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.services.dm.ingestion.ingestion_service import IngestionClient, IngestionService

#from ion.services.dm.ingest import IngestClient, IngestService
from ion.services.sa.fetcher import FetcherService

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

    def test_with_fetcher_service(self):
        """
        Use fetcher service to create dataset so we can test without
        actual messaging.
        """
        raise unittest.SkipTest('Not implemented')

        