#!/usr/bin/env python

"""
@file ion/play/rdf_store/test/test_rdf_service.py
@test ion.play.rdf_store/rdf_service 
@author David Stuebe
@Brief Test code for the Rdf Service
"""

from twisted.internet import defer

from ion.data.datastore.datastore_service import DataStoreServiceClient
from ion.test.iontest import IonTestCase

class DataStoreServiceTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_push_pull(self):

        services = [
            {'name':'DataStoreService1','module':'ion.data.datastore.datastore_service','class':'DataStoreService','spawnargs':{'MyFrontend':'afrontend'}},
        ]

        sup = yield self._spawn_processes(services)

        rsc = DataStoreServiceClient('localFrontend',proc=sup)
        yield rsc.push("LongKeyForRepo")
        
        yield rsc.pull("OtherLongKeyForRepo")

        yield rsc.clone("OtherLongKeyForRepo")
