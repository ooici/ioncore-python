#!/usr/bin/env python

"""
@file ion/services/coi/test/test_datastore.py
@author Michael Meisinger
@brief test datastore service
"""

import logging
from twisted.internet import defer
from twisted.trial import unittest

from ion.core import base_process
from ion.services.coi.attributestore import AttributeStoreService, AttributeStoreClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu


class DatastoreServiceTest(IonTestCase):
    """
    Testing service classes of data store
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_put(self):
        services = [
            {'name':'attstore1','module':'ion.services.coi.attributestore','class':'AttributeStoreService','spawnargs':{'servicename':'as1'}},
            {'name':'attstore2','module':'ion.services.coi.attributestore','class':'AttributeStoreService','spawnargs':{'servicename':'as2'}},
        ]

        sup = yield self._spawn_processes(services)

        asc1 = AttributeStoreClient(proc=sup, targetname='as1')

        res1 = yield asc1.put('key1','value1')
        logging.info('Result1 put: '+str(res1))

        res2 = yield asc1.get('key1')
        logging.info('Result2 get: '+str(res2))
        self.assertEqual(res2, 'value1')

        res3 = yield asc1.put('key1','value2')

        res4 = yield asc1.get('key1')
        self.assertEqual(res4, 'value2')

        res5 = yield asc1.get('non_existing')
        self.assertEqual(res5, None)

        asc2 = AttributeStoreClient(proc=sup, targetname='as2')

        resx1 = yield asc2.get('key1')
        self.assertEqual(resx1, None)
