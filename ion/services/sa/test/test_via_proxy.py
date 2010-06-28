#!/usr/bin/env python
"""
@file ion/services/sa/test/test_via_proxy.py
@author Paul Hubbard
@date 5/25/10
@test ion.services.sa.proxy Test of refactored proxy+fetcher+controller
@see http://bytes.com/topic/python/answers/22918-proxy-authentication-using-urllib2
@brief Designed to be an integration test, exercises DX via users' http proxy. Does
not use OOI messaging.
"""

from twisted.internet import defer
from twisted.internet import threads
from ion.test.iontest import IonTestCase
from twisted.trial import unittest

import logging
logging = logging.getLogger(__name__)

from twisted.internet import reactor
from twisted.web import client

from ion.core import ioninit
config = ioninit.config('ion.services.sa.proxy')
PROXY_PORT = int(config.getValue('proxy_port', '8100'))
import ion.util.procutils as pu

import urllib2

class IntegrationTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        services = [{'name':'fetcher',
                     'module':'ion.services.sa.fetcher',
                     'class': 'FetcherService'},
                    {'name': 'coordinator',
                     'module': 'ion.services.dm.preservation.coordinator',
                     'class' : 'CoordinatorService'},
                    {'name':'proxy',
                     'module': 'ion.services.sa.proxy',
                     'class': 'ProxyService'},]
        yield self._start_container()
        yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        # @note Required to trigger the slc_shutdown hook
        yield self._shutdown_processes()
        yield self._stop_container()

    def _get_page_with_proxy(self, src_url):
        """
        Simple routine analagous to twisted.web.client.getPage, but with
        support for a proxy. Note that urllib2 blocks the reactor, so this
        must be run in another thread!
        """
        proxy_support = urllib2.ProxyHandler(
            {"http" : "http://localhost:%d" % PROXY_PORT})
        opener = urllib2.build_opener(proxy_support)
        urllib2.install_opener(opener)

        f = urllib2.urlopen(src_url)
        page = f.read()
        f.close()
        return page

    @defer.inlineCallbacks
    def _get_page(self, src_url):
        """
        Run the pull in another thread, wait for the result.
        """
        doc = yield threads.deferToThread(self._get_page_with_proxy, src_url)
        defer.returnValue(doc)

    @defer.inlineCallbacks
    def test_start_stop(self):
        """
        If you want to manually test the proxy, increase this timeout and then
        telnet to the proxy port and issue strings like

        GET http://amoeba.ucsd.edu/tmp/test1.txt http/1.0

        """
        yield pu.asleep(0)

    @defer.inlineCallbacks
    def test_single_get(self):
        """
        Simplest test, fetch a fixed local page.
        @note Contents of same in /var/www/tmp on amoeba.ucsd.edu
        """
        #raise unittest.SkipTest('code not implemented yet')
        res = yield self._get_page('http://amoeba.ucsd.edu/tmp/test1.txt')
        self.failUnlessSubstring('Now is the time', res)

    @defer.inlineCallbacks
    def test_404(self):
        res = yield self._get_page('http://amoeba.ucsd.edu/fer-sure-404/')
        self.failUnlessEqual(res, '404: Not Found')

    @defer.inlineCallbacks
    def test_bad_host(self):
        res = yield self._get_page('http://antelopes-not-married.ucsd.edu/fer-sure-404/')
        self.failUnlessEqual(res, '404: Not Found')
