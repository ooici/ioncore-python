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
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_single_get(self):
        """
        Simplest test, fetch a fixed local page.
        @note Contenst of same in /var/www/tmp on amoeba.ucsd.edu
        """
        services = [{'name':'fetcher', 'module':'ion.data.fetcher',
                    'class': 'FetcherService'},]
        yield self._spawn_processes(services)

        dest = yield self.procRegistry.get('fetcher')
        logging.debug('fetcher: '+repr(dest))

        fc = FetcherClient()
        fc.attach()
        logging.debug('sending request...')
        res = yield fc.get_url(dest, 'http://amoeba.ucsd.edu/tmp/test1.txt')
        msg = res['value'].strip()
        self.failUnlessEqual(msg, 'Now is the time for all good men to come to the aid of their country.')
