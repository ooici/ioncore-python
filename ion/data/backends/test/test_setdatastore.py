#!/usr/bin/env python

"""
@file ion/data/backends/test/test_setdatastore.py
@author Paul Hubbard
@author Matt Rodriguez
@author David Stuebe
@test Service only test of the Set Cassandra datastore
"""


from twisted.trial import unittest
import logging
logging = logging.getLogger(__name__)
from uuid import uuid4
from ion.data.backends.setdatastore import SetCassandraStore
from twisted.internet import defer


class SetDataStoreInterfaceTest(unittest.TestCase):
    def setUp(self):
        clist = ['amoeba.ucsd.edu:9160']
        self.ds = SetCassandraStore(cass_host_list=clist)
        self.key = self._mkey()
        self.value = self._mkey()
        self.value2 = self._mkey()
    
    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.remove(self.key)
        logging.info(dir(self.ds._client))

    def _mkey(self):
        # Generate a pseudo-random string. handy, that.
        return str(uuid4())

    def test_get_404(self):
        # Make sure we can't read the not-written
        storeset = self.ds.smembers(self.key)
        self.failUnlessEqual(storeset, None)

    def test_sadd(self):
        # Hmm, simplest op, just looking for exceptions
        self.ds.sadd(self.key, self.value)

    def test_sremove(self):
        self.ds.sadd(self.key, self.value)
        self.ds.sremove(self.key, self.value)
        storeset = self.ds.smembers(self.key)
        self.failUnlessEqual(storeset, None)

    def test_scard(self):
        self.ds.sadd(self.key, self.value)
        card = self.ds.scard(self.key)
        self.failUnlessEqual(card, 1)

    def test_sadd_scard(self):
        """
        Ensures that the set is a collection of unique elements

        Add the same element twice to the set, check to see that
        cardinality is one.
        """
        self.ds.sadd(self.key, self.value)
        self.ds.sadd(self.key, self.value)
        card = self.ds.scard(self.key)
        self.failUnlessEqual(card, 1)

    def test_smembers(self):
        """
        Adds two elements to the set then retrieves them.
        """
        self.ds.sadd(self.key, self.value)
        self.ds.sadd(self.key, self.value2)
        members = self.ds.smembers(self.key)
        self.failUnlessEqual(members, set([self.value, self.value2]))
