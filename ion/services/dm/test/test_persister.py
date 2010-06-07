#!/usr/bin/env python

"""
@file ion/services/dm/test/test_persister.py
@test ion.services.dm.persister Persister unit tests
@author Paul Hubbard
@date 6/7/10
"""

import logging

from twisted.internet import defer
from twisted.trial import unittest

from ion.services.dm.persister import PersisterClient, PersisterService
from ion.services.dm.fetcher import FetcherService

from ion.test.iontest import IonTestCase

class PersisterServiceTester(unittest.TestCase):
    """
    Create an instance of the persister service class and test same w/out capability
    container & messaging. Also instantiates the fetcher the same way to pull
    a dataset.

    Way too clever.
    """
    def catch_reply(self, msg, dset_msg):
        self.buffer = dset_msg

    def catch_err(self, msg, payload, headers):
        self.error_buffer = payload

    def setUp(self):
        """
        Instantiate the service classes
        """
        self.ps = PersisterService()
        self.fs = FetcherService()
        # Create a local buffer/array to write into
        self.buffer = {}
        self.error_buffer = {}
        # Monkeypatch the classes to call our functions when complete
        self.fs.reply_ok = self.catch_reply
        self.ps.reply_err = self.catch_err

    def test_updown(self):
        pass

    @defer.inlineCallbacks
    def test_fetcher_and_persister(self):
        dset_url = 'http://ooici.net:8001/coads.nc'
        dset = self.fs.op_get_dap_dataset(dset_url, None, None)
        yield self.ps.op_persist_dap_dataset(dset_url, dset, None)
        self.fail('Check local dir for file coads.nc!')


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
        services = [
            {'name': 'persister', 'module': 'ion.services.dm.persister',
             'class': 'PersisterService'},
        ]
        sup = yield self._spawn_processes(services)

        pc = PersisterClient(proc=sup)
        rc = yield pc.persist_dap_dataset('none')
        self.failUnlessSubstring('No code yet!', rc)
