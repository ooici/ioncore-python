#!/usr/bin/env python
"""
@file ion/services/sa/test/test_fetcher.py
@author Paul Hubbard
@date 5/7/10
@test ion.services.sa.fetcher Test of rewritten fetcher
"""

import base64
import logging
log = logging.getLogger(__name__)
from twisted.internet import defer
from socket import gaierror
from twisted.trial import unittest

from ion.services.sa.fetcher import FetcherClient, FetcherService
from ion.test.iontest import IonTestCase

class FetcherServiceTester(IonTestCase):
    """
    Just instantiate the FetcherService class and exercise the inner get_page
    method.
    """
    def setUp(self):
        self.mf = FetcherService()

    def test_single_page_with_headers(self):
        page = self.mf.get_page('http://amoeba.ucsd.edu/tmp/test1.txt',
                                get_headers=True)
        self.failUnlessSubstring('content-length', page)
        self.failUnlessSubstring('is the time for all', page)

    def test_no_headers(self):
        page = self.mf.get_page('http://amoeba.ucsd.edu/tmp/test1.txt')
        self.failIfSubstring('content-length', page)
        self.failUnlessSubstring('is the time for all', page)

    def test_bad_host(self):
        try:
            self.mf.get_page('http://foo.bar.baz/')
        except gaierror, ge:
            log.debug('got err as expected! %s' % str(ge))
            pass
        except ValueError, ve:
            log.debug('got err as expected: %s' % str(ve))
            pass
        else:
            self.fail('Should have raised an exception!')

    def test_404(self):
        self.failUnlessRaises(ValueError, self.mf.get_page,
                              'http://amoeba.ucsd.edu/tmp/bad-filename')

class FetcherTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [{'name':'fetcher', 'module':'ion.services.sa.fetcher',
                    'class': 'FetcherService'},]
        sup = yield self._spawn_processes(services)
        self.fc = FetcherClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def _get_page(self, src_url):
        log.debug('sending GET request for "%s"...' % src_url)
        res = yield self.fc.get_url(src_url)
        if res['status'] == 'ERROR':
            raise ValueError('Error on fetch')
        msg = base64.b64decode(res['value'])
        defer.returnValue(msg)

    @defer.inlineCallbacks
    def _get_phead(self, src_url):
        log.debug('sending HEAD request for "%s"...' % src_url)
        res = yield self.fc.get_head(src_url)
        if res['status'] == 'ERROR':
            raise ValueError('Error on fetch')
        msg = base64.b64decode(res['value'])
        defer.returnValue(msg)

    ###############################################
    def test_instantiation_only(self):
        pass

    @defer.inlineCallbacks
    def test_single_get(self):
        """
        Simplest test, fetch a fixed local page.
        @note Contenst of same in /var/www/tmp on amoeba.ucsd.edu
        """
        res = yield self._get_page('http://amoeba.ucsd.edu/tmp/test1.txt')
        self.failUnlessSubstring('Now is the time for all good men', res)
        self.failUnlessSubstring('content-length', res)

    @defer.inlineCallbacks
    def test_page_head(self):
        """
        Similar to get, but using HEAD verb to just pull headers.
        """
        res = yield self._get_phead('http://amoeba.ucsd.edu/tmp/test1.txt')
        self.failUnlessSubstring('content-length', res)

    @defer.inlineCallbacks
    def test_404(self):
        try:
            yield self._get_page('http://ooici.net/404-fer-sure')
            self.fail('Should have gotten an exception for 404 error!')
        except ValueError:
            pass

    @defer.inlineCallbacks
    def test_header_404(self):
        try:
            yield self._get_phead('http://ooici.net/404-fer-sure')
            self.fail('Should have gotten an exception for 404 error!')
        except ValueError:
            pass

TEST_DSET = 'http://ooici.net:8001/coads.nc'
TEST_ADSET1 = 'http://ooici.net:8001/grid_surf_el1.nc'
TEST_ADSET2 = 'http://ooici.net:8001/grid_surf_el2.nc'
TEST_APATTERN = "/tmp/grid_surf_el*.nc"

class TransportTester(IonTestCase):
    """
    Verify that we can transport binary (XDR) data.
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 120

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_fetcher_service_only(self):
        raise unittest.SkipTest('Timing out on EC2')

        """
        Use fetcher service to get a complete dataset, try to decode the message
        fields once we get it.
        """
        services = [
             {'name': 'fetcher', 'module': 'ion.services.sa.fetcher',
             'class': 'FetcherService'},
            ]

        sup = yield self._spawn_processes(services)

        fc = FetcherClient(proc=sup)
        dset = yield fc.get_dap_dataset(TEST_DSET)

        # decode fields as an integrity test
        dset['das'] = json.loads(dset['das'])
        dset['dds'] = json.loads(dset['dds'])
        dset['dods'] = base64.b64decode(dset['dods'])
        self.failUnlessEqual(dset['source_url'], TEST_DSET)
        self.failUnlessSubstring('COADSX', dset['das'])
        self.failUnlessSubstring('COADSY', dset['das'])


