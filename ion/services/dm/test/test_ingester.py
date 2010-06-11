#!/usr/bin/env python

"""
@file ion/services/dm/test/test_ingester.py
@author Paul Hubbard
@date 6/11/10
"""

import logging
logging = logging.getLogger(__name__)


from twisted.internet import defer
from twisted.trial import unittest
from ion.test.iontest import IonTestCase

from ion.services.dm.ingest import IngestClient, IngestService
from ion.services.sa.fetcher import FetcherService

class IngesterTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 60
        services = [
            {'name': 'ingest', 'module': 'ion.services.dm.ingest',
             'class':'IngestService'}
        ]
        sup = yield self._spawn_processes(services)
        self.ic = IngestClient(proc=sup)
        self.fs = FetcherService()
        self.mys = IngestService()

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
        dset_url = 'http://ooici.net:8001/coads.nc'
        dset = self.fs._get_dataset_no_xmit(dset_url)
        logging.debug('Sending dataset to ingester!')

        mdict = self.mys._do_ingest(dset)

        self.failUnlessSubstring('TIME', mdict)
        self.failUnlessSubstring('COADSX', mdict)
        self.failUnlessSubstring('COADSY', mdict)
        self.failUnlessSubstring('SST', mdict)
