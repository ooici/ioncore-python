#!/usr/bin/env python

"""
@file ion/data/test/test_store.py
@author Paul Hubbard
@author Dorian Raymer
@test Service only test of Cassandra datastore
"""

import logging
from uuid import uuid4

from twisted.trial import unittest
from twisted.internet import defer

from ion.data.store import CassandraStore

class CassandraStoreTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.WARN, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')
        clist = ['amoeba.ucsd.edu:9160']
        self.ds = CassandraStore(cass_host_list=clist)
        self.ds.init()
        self.key = self._mkey()
        self.value = self._mkey()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.delete(self.key)
        del self.ds

    def _mkey(self):
        # Generate a pseudo-random string. handy, that.
        return str(uuid4())

    @defer.inlineCallbacks
    def test_get_404(self):
        # Make sure we can't read the not-written
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.ds.put(self.key, self.value)

    @defer.inlineCallbacks
    def test_delete(self):
        yield self.ds.put(self.key, self.value)
        yield self.ds.delete(self.key)
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_put_get_delete(self):
        # Write, then read to verify same
        yield self.ds.put(self.key, self.value)
        b = yield self.ds.get(self.key)
        self.failUnlessEqual(self.value, b)

    @defer.inlineCallbacks
    def test_query(self):
        # Write a key, query for it, verify contents
        yield self.ds.put(self.key, self.value)
        rl = yield self.ds.query(self.key)
        self.failUnlessEqual(rl[0][0], self.key)
