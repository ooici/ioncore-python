#!/usr/bin/env python

"""
@file ion/data/datastore/test/test_datastore_service.py
@test ion.data.datastore
@author David Stuebe
@brief Test code for the datastore Service
"""

from twisted.internet import defer
from twisted.trial import unittest

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
        raise unittest.SkipTest('code not completed')

        services = [
            {'name':'DataStoreService1','module':'ion.data.datastore.datastore_service','class':'DataStoreService','spawnargs':{'MyFrontend':'afrontend'}},
        ]

        sup = yield self._spawn_processes(services)

        rsc = DataStoreServiceClient('localFrontend',proc=sup)
        yield rsc.push("LongKeyForRepo")

        yield rsc.pull("OtherLongKeyForRepo")

        yield rsc.clone("OtherLongKeyForRepo")
