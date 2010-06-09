#!/usr/bin/env python

"""
@file ion/services/dm/test/test_persister.py
@test ion.services.dm.persister Persister unit tests
@author Paul Hubbard
@date 6/7/10
"""
import sys

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest

from ion.services.dm.persister import PersisterClient, PersisterService
from ion.services.dm.fetcher import FetcherService
from ion.services.dm.url_manipulation import generate_filename

from ion.test.iontest import IonTestCase

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

    def test_fetcher_and_persister(self):
        """
        More complex than it might appear - reach in and use the methods
        to get and persist a full dataset from amoeba (5.2MB)
        """
        dset_url = 'http://ooici.net:8001/coads.nc'
        local_dir = '/tmp/'
        fname = generate_filename(dset_url, local_dir=local_dir)

        logging.debug('Grabbing dataset ' + dset_url)
        dset = self.fs._get_dataset_no_xmit(dset_url)
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
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_updown(self):
        raise unittest.SkipTest('Code broken')

        services = [
            {'name': 'persister', 'module': 'ion.services.dm.persister',
             'class': 'PersisterService'},
        ]
        sup = yield self._spawn_processes(services)

        raise unittest.SkipTest('Code broken')

        pc = PersisterClient(proc=sup)
        rc = yield pc.persist_dap_dataset('none')
        self.failUnlessSubstring('No code yet!', rc)
