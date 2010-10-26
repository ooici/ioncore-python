#!/usr/bin/env python

"""
@file ion/services/coi/test/test_hello.py
@author David Stuebe
"""

from twisted.internet import defer

from ion.services.coi.datastore import DataStoreServiceClient
from ion.test.iontest import IonTestCase

class DataStoreTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService'},
        ]

        sup = yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello(self):

        dsc = DataStoreServiceClient(proc=sup)
        yield dsc.hello("Hi there, hello1")
