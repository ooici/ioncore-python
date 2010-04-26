#!/usr/bin/env python

"""
@file ion/data/test/test_datastore.py
@author Paul Hubbard
@test Service only test of Cassandra datastore
"""


from twisted.trial import unittest
import logging
from uuid import uuid4

from ion.data.cassandrads import CassandraStore

class DatastoreTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.WARN, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')
        clist = ['amoeba.ucsd.edu:9160']
        self.ds = CassandraStore(cass_host_list=clist)
        self.key = self._mkey()
        self.value = self._mkey()

    def tearDown(self):
        self.ds.delete(self.key)
        del self.ds

    def _mkey(self):
        # Generate a pseudo-random string. handy, that.
        return str(uuid4())

    def test_get_404(self):
        # Make sure we can't read the not-written
        rc = self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        self.ds.put(self.key, self.value)

    def test_put_get_delete(self):
        # Write, then read to verify same
        self.ds.put(self.key, self.value)
        b = self.ds.get(self.key)
        self.failUnlessEqual(self.value, b)

    def test_query(self):
        # Write a key, query for it, verify contents
        self.ds.put(self.key, self.value)
        rl = self.ds.query(self.key)
        self.failUnlessEqual(rl[0][0], self.key)

class DatastoreTestNoArgsConstructor(unittest.TestCase):
    """
    Same tests, but using the no-args constructor.
    """
    def setUp(self):
        """
        @note the tests all seemed to use a random key and value, so might
        as well factor that code out.
        """
        logging.basicConfig(level=logging.WARN, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')
        self.ds = CassandraStore()
        self.key = self._mkey()
        self.value = self._mkey()

    def tearDown(self):
        """
        @note Some tests have not written, but s'ok, no worries if we try
        to delete a non-existent key
        """
        self.ds.delete(self.key)
        del self.ds

    def _mkey(self):
        # Generate a pseudo-random string. handy, that.
        return str(uuid4())

    def test_get_404(self):
        # Make sure we can't read the not-written
        rc = self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        self.ds.put(self.key, self.value)

    def test_put_get_delete(self):
        # Write, then read to verify same
        self.ds.put(self.key, self.value)
        b = self.ds.get(self.key)
        self.failUnlessEqual(self.value, b)

    def test_query(self):
        # Write a key, query for it, verify contents
        self.ds.put(self.key, self.value)
        rl = self.ds.query(self.key)
        self.failUnlessEqual(rl[0][0], self.key)
