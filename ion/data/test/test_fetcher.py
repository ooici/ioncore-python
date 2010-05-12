#!/usr/bin/env python
"""
@file ion/data/test/test_fetcher.py
@author Paul Hubbard
@date 5/7/10
@test ion.data.fetcher Test of refactored fetcher
"""

import logging
from twisted.internet import defer

from ion.data.fetcher import FetcherClient
from ion.test.iontest import IonTestCase

class DatastoreTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [{'name':'fetcher', 'module':'ion.data.fetcher',
                    'class': 'FetcherService'},]
        yield self._spawn_processes(services)

        self.dest = yield self.procRegistry.get('fetcher')
        self.fc = FetcherClient()
        self.fc.attach()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def _get_page(self, src_url):
        logging.debug('sending request for "%s"...' % src_url)
        res = yield self.fc.get_url(self.dest, src_url)
        msg = res['value']
        defer.returnValue(msg)

    @defer.inlineCallbacks
    def test_single_get(self):
        """
        Simplest test, fetch a fixed local page.
        @note Contenst of same in /var/www/tmp on amoeba.ucsd.edu
        """
        res = yield self._get_page('http://amoeba.ucsd.edu/tmp/test1.txt')
        msg = res.strip()
        self.failUnlessEqual(msg, 'Now is the time for all good men to come to the aid of their country.')

    @defer.inlineCallbacks
    def test_404(self):
        self.failUnlessFailure(self._get_page('http://ooici.net/404-fer-sure'))
