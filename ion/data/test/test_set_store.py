#!/usr/bin/env python

"""
@file ion/data/backends/test/test_set_store.py
"""

import logging
from uuid import uuid4

from twisted.trial import unittest
from twisted.internet import defer

from ion.data.set_store import SetStore


class ISetStoreTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        
        self.ds = yield SetStore.create_set_store()
        self.key = str(uuid4())
        self.value1 = str(uuid4())
        self.value2 = str(uuid4())

    def tearDown(self):
        #yield self.ds.delete(self.key)
        #del self.ds
        pass

    @defer.inlineCallbacks
    def test_get_404(self):
        # Make sure we can't read the not-written
        rc = yield self.ds.smembers(self.key)
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.ds.sadd(self.key, self.value1)

    @defer.inlineCallbacks
    def test_delete(self):
        yield self.ds.sadd(self.key, self.value1)
        yield self.ds.remove(self.key)
        rc = yield self.ds.smembers(self.key)
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_put_get_delete(self):
        # Write, then read to verify same
        yield self.ds.sadd(self.key, self.value1)
        yield self.ds.sadd(self.key, self.value2)
        b = yield self.ds.smembers(self.key)
        self.failUnlessEqual(set([self.value1,self.value2]), b)

    @defer.inlineCallbacks
    def test_query(self):
        # Write a key, query for it, verify contents
        yield self.ds.sadd(self.key, self.value1)
        rl = yield self.ds.query(self.key)
        self.failUnlessEqual(rl[0], self.key)



#class CassandraStoreTest(IStoreTest):
#
#    @defer.inlineCallbacks
#    def setUp(self):
#        clist = ['amoeba.ucsd.edu:9160']
#        self.ds = yield cassandra.CassandraStore.create_store(cass_host_list=clist)
#        self.key = str(uuid4())
#        self.value = str(uuid4())


