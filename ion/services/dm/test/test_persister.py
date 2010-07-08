#!/usr/bin/env python

"""
@file ion/services/dm/test/test_persister.py
@test ion.services.dm.persister Persister unit tests
@author Paul Hubbard
@date 6/7/10
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest

from ion.services.dm.persister import PersisterClient, PersisterService
from ion.services.sa.fetcher import FetcherService, FetcherClient
from ion.services.dm.url_manipulation import generate_filename

from ion.test.iontest import IonTestCase
import base64
import simplejson as json

TEST_DSET = 'http://ooici.net:8001/coads.nc'
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


class ServiceTester(unittest.TestCase):
    """
    Create an instance of the fetcher and persister services
    and test w/out capability container & messaging.

    Way too clever.
    """
    def setUp(self):
        """
        Instantiate the service classes
        """
        self.ps = PersisterService()
        self.fs = FetcherService()
        self.timeout = 120

    def test_instantiation_only(self):
        # Create and destroy the instances - any errors?
        pass

    def test_fetcher_and_persister_no_messaging(self):
        """
        More complex than it might appear - reach in and use the methods
        to get and persist a full dataset from amoeba (5.2MB)
        """
        # generate filename so we can look for it after saving
        local_dir = '/tmp/'
        fname = generate_filename(TEST_DSET, local_dir=local_dir)

        dset = self.fs._get_dataset_no_xmit(TEST_DSET)
        self.ps._save_no_xmit(dset, local_dir=local_dir)

        f = open(fname, 'r')
        if f:
           f.close()
        else:
            self.fail('Datafile not found!')

class PersisterTester(IonTestCase):
    """
    Exercise the persister.
    """
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 300
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_fetcher_svc_persister_client(self):
        raise unittest.SkipTest('Timing out on EC2')
        """
        Trying to track down a failure - use fetcher service and
        persister client.
        """
        services = [
            {'name': 'persister', 'module': 'ion.services.dm.persister',
             'class': 'PersisterService'},
            ]
        boss = yield self._spawn_processes(services)
        fs = FetcherService()
        dset = fs._get_dataset_no_xmit(TEST_DSET)
        pc = PersisterClient(proc=boss)
        rc = yield pc.persist_dap_dataset(dset)
        self.failUnlessSubstring('OK', rc)

    @defer.inlineCallbacks
    def test_svcs_and_messaging(self):
        raise unittest.SkipTest('Timing out on EC2')
        services = [
            {'name': 'persister', 'module': 'ion.services.dm.persister',
             'class': 'PersisterService'},
            {'name': 'fetcher', 'module': 'ion.services.sa.fetcher',
             'class': 'FetcherService'},
        ]
        boss = yield self._spawn_processes(services)

        fc = FetcherClient(proc=boss)
        logging.debug('Grabbing dataset ' + TEST_DSET)
        dset = yield fc.get_dap_dataset(TEST_DSET)

        pc = PersisterClient(proc=boss)
        #ps = PersisterService()
        logging.debug('Saving dataset...')
        rc = yield pc.persist_dap_dataset(dset)
        #ps._save_no_xmit(dset)
        self.failUnlessSubstring('OK', rc)
