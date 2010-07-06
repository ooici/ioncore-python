#!/usr/bin/env python

"""
@file ion/services/dm/test/test_persister.py
@test ion.services.dm.persister Persister unit tests
@author Paul Hubbard
@date 6/7/10
"""

import logging
logging = logging.getLogger(__name__)

import httplib as http
import urlparse

from twisted.internet import defer
from twisted.trial import unittest

from ion.play.hello_service import HelloServiceClient

from ion.services.dm.persister import PersisterClient, PersisterService
from ion.services.sa.fetcher import FetcherService, FetcherClient
from ion.services.dm.url_manipulation import generate_filename

from ion.test.iontest import IonTestCase
import base64

class TransportTester(IonTestCase):
    """
    Verify that carrot and ion can transport binary (XDR) data.
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 60

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_one(self):
        #raise unittest.SkipTest('Waiting for CISWCORE-14')

        services = [
             {'name': 'fetcher', 'module': 'ion.services.sa.fetcher',
             'class': 'FetcherService'},
            ]

        sup = yield self._spawn_processes(services)

        fc = FetcherClient(proc=sup)
        dset = yield fc.get_dap_dataset('http://ooici.net:8001/coads.nc')
        dset = base64.b64decode(dset)
        #logging.warn(dset)
        logging.warn(type(dset))
        self.failUnlessSubstring(dset, 'NC_GLOBAL')
        self.failUnlessSubstring(dset, 'ooi-download-timestamp')
        self.failUnlessSubstring(dset, 'ooi-source-url')
        self.failUnlessSubstring(dset, 'http://ooici.net:8001/coads.nc')

    @defer.inlineCallbacks
    def test_two(self):
        raise unittest.SkipTest('Waiting for CISWCORE-14')

        """
        Try a manual pull of XDR and send to known-good service.
        """

        services = [
            {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
        ]

        sup = yield self._spawn_processes(services)

        hc = HelloServiceClient(proc=sup)

        src = urlparse.urlsplit('http://amoeba.ucsd.edu:8001/glacier.nc?var232%5B0:1:0%5D%5B0:1:0%5D%5B0:1:601%5D%5B0:1:401%5D&')
        conn = http.HTTPConnection(src.netloc)
        conn.request('GET', src.path)
        res = conn.getresponse()
        payload = res.read()

        yield hc.hello(payload)

class PersisterServiceTester(unittest.TestCase):
    """
    Create an instance of the persister service class and test same w/out capability
    container & messaging. Also instantiates the fetcher the same way to pull
    a dataset.

    Way too clever.
    """
    def setUp(self):
        """
        Instantiate the service classes
        """
        self.ps = PersisterService()
        self.fs = FetcherService()

    def test_instantiation_only(self):
        # Create and destroy the instances - any errors?
        pass

    def test_fetcher_and_persister_no_messaging(self):
        """
        More complex than it might appear - reach in and use the methods
        to get and persist a full dataset from amoeba (5.2MB)
        """
        dset_url = 'http://ooici.net:8001/coads.nc'
        local_dir = '/tmp/'
        fname = generate_filename(dset_url, local_dir=local_dir)

        logging.debug('Grabbing dataset ' + dset_url)
        dset = self.fs._get_dataset_no_xmit(dset_url)
        logging.debug('decoding b64-dods')
        dset['dods'] = base64.b64decode(dset['dods'])
        logging.debug('Persisting dataset')
        self.ps._save_no_xmit(dset, local_dir=local_dir)
        if open(fname):
           pass
        else:
            self.fail('Datafile not found!')

class PersisterTester(IonTestCase):
    """
    Exercise the persister.
    """
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 30
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_fetcher_and_persister_services(self):
        #raise unittest.SkipTest('Waiting for CISWCORE-14')

        services = [
            {'name': 'persister', 'module': 'ion.services.dm.persister',
             'class': 'PersisterService'},
            {'name': 'fetcher', 'module': 'ion.services.sa.fetcher',
             'class': 'FetcherService'},
        ]
        sup = yield self._spawn_processes(services)

        dset_url = 'http://ooici.net:8001/coads.nc'
        local_dir = '/tmp/'
        fname = generate_filename(dset_url, local_dir=local_dir)

        pc = PersisterClient(proc=sup)
        fc = FetcherClient(proc=sup)
        fs = FetcherService()

        logging.debug('Grabbing dataset ' + dset_url)
        dset = fs._get_dataset_no_xmit(dset_url)

        rc = yield pc.persist_dap_dataset(dset)
        self.failUnlessSubstring('SUCCESS', rc)
