#!/usr/bin/env python

"""
@file ion/data/backends/test/test_store_service.py
@test ion.data.backends.store_service test cases
@author David Stuebe
"""

from twisted.internet import defer

from ion.data.backends.store_service import StoreServiceClient
from ion.data.test import test_store
from ion.test.iontest import IonTestCase

class StoreServiceTest(IonTestCase, test_store.IStoreTest):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def _setup_backend(self):
        yield self._start_container()
        services = [
            {'name':'store1','module':'ion.data.backends.store_service','class':'StoreService'},
        ]

        sup = yield self._spawn_processes(services)
        ds = yield StoreServiceClient.create_store(proc=sup)
        
        defer.returnValue(ds)
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
