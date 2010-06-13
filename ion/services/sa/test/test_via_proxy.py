#!/usr/bin/env python
"""
@file ion/services/sa/test/test_via_proxy.py
@author Paul Hubbard
@date 5/25/10
@test ion.services.sa.proxy Test of refactored proxy+fetcher+controller
@note Complete lack of DX code, just stock python calls!
@see http://bytes.com/topic/python/answers/22918-proxy-authentication-using-urllib2
@brief Designed to be an integration test, exercises DX via users' http proxy. Does
not use OOI messaging.
"""

import urllib2

from twisted.internet import defer
from ion.test.iontest import IonTestCase
from twisted.trial import unittest

import logging
logging = logging.getLogger(__name__)

class IntegrationTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        services = [{'name':'fetcher',
                     'module':'ion.services.sa.fetcher',
                     'class': 'FetcherService'},
                    {'name': 'coordinator',
                     'module': 'ion.services.dm.coordinator',
                     'class' : 'CoordinatorService'},
                    {'name':'proxy',
                     'module': 'ion.services.sa.proxy',
                     'class': 'ProxyService'},]
        yield self._start_container()
        yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def _get_page(self, src_url):
        ph = urllib2.ProxyHandler({'http':'http://localhost:8000'})
        opener = urllib2.build_opener(ph)
        urllib2.install_opener(opener)

        logging.debug('sending request for "%s" via proxy...' % src_url)
        fh = urllib2.urlopen(src_url)
        page = fh.read()
        fh.close()
        return page

    def test_single_get(self):
        """
        Simplest test, fetch a fixed local page.
        @note Contents of same in /var/www/tmp on amoeba.ucsd.edu
        """
        raise unittest.SkipTest('code not implemented yet')
        res = self._get_page('http://amoeba.ucsd.edu/tmp/test1.txt')
        self.failUnlessSubstring('Now is the time', res)

    @defer.inlineCallbacks
    def _test_404(self):
        raise unittest.SkipTest('code not implemented yet')
