#!/usr/bin/env python
"""
@file ion/data/test/test_fetcher.py
@author Paul Hubbard
@date 5/7/10
@test ion.data.fetcher Test of refactored fetcher
"""

from twisted.trial import unittest
import logging
from twisted.internet import defer

from ion.data.fetcher import FetcherClient, FetcherService
from magnet.spawnable import spawn
from ion.test.iontest import IonTestCase

class DatastoreTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._startContainer()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stopContainer()

    @defer.inlineCallbacks
    def test_single_get(self):
        services = [{'name':'fetcher', 'module':'ion.data.fetcher',
                    'class': 'FetcherService'},]
        yield self._spawnProcesses(services)

        sup = yield self.procRegistry.get('fetcher')
        logging.info('Supervisor: '+repr(sup))

        fc = FetcherClient()
        fc.attach()
        logging.info('sending request...')
        res = yield fc.get_url('http://amoeba.ucsd.edu/tmp/test1.txt')
        logging.info(res)
