#!/usr/bin/env python

"""
@file ion/play/test/test_data_acquisition.py
@test ion.services.sa.data_acquisition Example unit tests for sample code.
@author Michael Meisinger
"""

from twisted.internet import defer

from ion.services.sa.data_acquisition import DataAcquisitionServiceClient
from ion.services.sa.data_acquisition import DataAcquisitionDataStore
from ion.test.iontest import IonTestCase

from twisted.trial import unittest

class DataAcquisitionTest(IonTestCase):
    """
    Testing example data acquisition service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'data_acquisition','module':'ion.services.sa.data_acquisition','class':'DataAcquisitionService'},
        ]

        sup = yield self._spawn_processes(services)

        self.da = DataAcquisitionServiceClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_acquire_message(self):

         yield self.da.acquire_message("accessing acquire message")

    @defer.inlineCallbacks
    def test_acquire_block(self):

         yield self.da.acquire_block("accessing acquire block")

 
    @defer.inlineCallbacks
    def test_push_pull(self):
        raise unittest.SkipTest('code not completed')
        
        services = [
            {'name':'DataStoreService1','module':'ion.data.datastore.datastore_service','class':'DataStoreService','spawnargs':{'MyFrontend':'afrontend'}},
        ]

        sup = yield self._spawn_processes(services)

        rsc = DataAcquisitionDataStore('localFrontend',proc=sup)
        
        yield self.rsc.test_data_store()
        
        
