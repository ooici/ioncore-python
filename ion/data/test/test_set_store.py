#!/usr/bin/env python

"""
@file ion/data/backends/test/test_set_store.py
"""

import logging
logging = logging.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
from twisted.internet import defer

from ion.data.set_store import SetStore


class ISetStoreTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        
        self.ds = yield SetStore.create_store()
        self.key = str(uuid4())
        self.value1 = str(uuid4())
        self.value2 = str(uuid4())

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.remove(self.key)

    @defer.inlineCallbacks
    def test_smembers(self):
        #key should not exist, empty set expected
        empty_set = yield self.ds.smembers(self.key)
        self.failUnlessEqual(empty_set, set()) #should be an empty set

        test1 = 'test1'
        test2 = 'test2'
        test_set = set([test1, test2])
        yield self.ds.sadd(self.key, test1)
        yield self.ds.sadd(self.key, test2)

        nonempty_set = yield self.ds.smembers(self.key)
        self.failUnlessEqual(test_set, nonempty_set)

    @defer.inlineCallbacks
    def test_srandmember(self):
        key1 = 'key1'
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        s1 = set([test1, test2, test3])
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)
        yield self.ds.sadd(key1, test3)

        elm = yield self.ds.srandmember(key1)
        self.failUnlessIn(elm, s1)

        res = yield self.ds.sismember(key1, elm) 
        self.failUnless(res)

        yield self.ds.remove(key1)

    @defer.inlineCallbacks
    def test_sadd(self):
        test1 = 'test1'
        before_add = yield self.ds.smembers(self.key)
        self.failIf(before_add)
        yield self.ds.sadd(self.key, test1)
        after_add = yield self.ds.smembers(self.key)
        self.failUnlessIn(test1, after_add)

    @defer.inlineCallbacks
    def test_sremove(self):
        test1 = 'test1'
        test2 = 'test2'
        yield self.ds.sadd(self.key, test1)
        yield self.ds.sadd(self.key, test2)
        res = yield self.ds.smembers(self.key)
        self.failUnlessIn(test1, res)
        yield self.ds.sremove(self.key, test1)
        res = yield self.ds.smembers(self.key)
        self.failIfIn(test1, res)
        self.failUnlessIn(test2, res)

    @defer.inlineCallbacks
    def test_remove(self):
        """
        @note remove removes an entire set in the store; sremove removes a member
        from a set
        """
        yield self.ds.sadd(self.key, self.value1)
        yield self.ds.remove(self.key)
        rc = yield self.ds.smembers(self.key)
        self.failUnlessEqual(rc, set())

    @defer.inlineCallbacks
    def test_spop(self):
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        yield self.ds.sadd(self.key, test1)
        yield self.ds.sadd(self.key, test2)
        yield self.ds.sadd(self.key, test3)
        elm = yield self.ds.spop(self.key)
        self.failUnlessIn(elm, set([test1, test2, test3]))

        test_set = yield self.ds.smembers(self.key)
        self.failIfIn(elm, test_set)

    @defer.inlineCallbacks
    def test_smove(self):
        key1 = 'key1'
        key2 = 'key2'
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)
        yield self.ds.sadd(key1, test3)

        res = yield self.ds.smove(key1, key2, test1)
        self.failUnless(res)

        res = yield self.ds.smove(key1, key2, test1)
        self.failIf(res)

        res = yield self.ds.smembers(key2)
        self.failUnlessIn(test1, res)

        yield self.ds.remove(key1)
        yield self.ds.remove(key2)

    @defer.inlineCallbacks
    def test_scard(self):
        key1 = 'key1'
        res = yield self.ds.scard(key1)
        self.failUnlessEqual(res, 0)

        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        s1 = set([test1, test2, test3])
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)
        yield self.ds.sadd(key1, test3)
        res = yield self.ds.scard(key1)
        self.failUnlessEqual(res, len(s1))

        yield self.ds.remove(key1)

    @defer.inlineCallbacks
    def test_sismember(self):
        test1 = 'test1'
        test2 = 'test2'

        res = yield self.ds.sismember(self.key, test1)
        self.failIf(res)

        yield self.ds.sadd(self.key, test1)
        res = yield self.ds.sismember(self.key, test2)
        self.failIf(res)
        res = yield self.ds.sismember(self.key, test1)
        self.failUnless(res)

    @defer.inlineCallbacks
    def test_sinter(self):
        key1 = 'key1'
        key2 = 'key2'
        key3 = 'key3'
        key4 = 'key4'
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        s1 = set([test1, test2, test3])
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)
        yield self.ds.sadd(key1, test3)

        # equivalent to smembers
        res = yield self.ds.sinter(key1)
        self.failUnlessEqual(res, s1)

        # should be empty set
        res = yield self.ds.sinter(key1, key2)
        self.failUnlessEqual(res, set())

        s2 = set([test2, test3])
        yield self.ds.sadd(key2, test2)
        yield self.ds.sadd(key2, test3)
        res = yield self.ds.sinter(key1, key2)
        self.failUnlessEqual(res, s2)

        res = yield self.ds.sinter(key1, key2, key3)
        self.failUnlessEqual(res, set())

        yield self.ds.remove(key1)
        yield self.ds.remove(key2)

    @defer.inlineCallbacks
    def test_sinterstore(self):
        key1 = 'key1'
        key2 = 'key2'
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        s1 = set([test1, test2, test3])
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)
        yield self.ds.sadd(key1, test3)

        s2 = set([test2, test3])
        yield self.ds.sadd(key2, test2)
        yield self.ds.sadd(key2, test3)

        dstkey = 'dstkey'
        yield self.ds.sinterstore(dstkey, key1, key2)
        res = yield self.ds.smembers(dstkey)
        self.failUnlessEqual(s2, res)

        yield self.ds.remove(key1)
        yield self.ds.remove(key2)
        yield self.ds.remove(dstkey)

    @defer.inlineCallbacks
    def test_sunion(self):
        key1 = 'key1'
        key2 = 'key2'
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        s1 = set([test1, test2])
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)

        res = yield self.ds.sunion(key1)
        self.failUnlessEqual(res, s1)

        s2 = set([test2, test3])
        yield self.ds.sadd(key2, test2)
        yield self.ds.sadd(key2, test3)

        res = yield self.ds.sunion(key1, key2)
        self.failUnlessEqual(res, s1.union(s2))

        yield self.ds.remove(key1)
        yield self.ds.remove(key2)

    @defer.inlineCallbacks
    def test_sunionstore(self):
        key1 = 'key1'
        key2 = 'key2'
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        s1 = set([test1, test2])
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)

        s2 = set([test2, test3])
        yield self.ds.sadd(key2, test2)
        yield self.ds.sadd(key2, test3)

        dstkey = 'dstkey'
        yield self.ds.sunionstore(dstkey, key1, key2)
        res = yield self.ds.sunion(dstkey)
        self.failUnlessEqual(res, s1.union(s2))

        yield self.ds.remove(key1)
        yield self.ds.remove(key2)
        yield self.ds.remove(dstkey)

    @defer.inlineCallbacks
    def test_sdiff(self):
        key1 = 'key1'
        key2 = 'key2'
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        s1 = set([test1, test2])
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)

        s2 = set([test2, test3])
        yield self.ds.sadd(key2, test2)
        yield self.ds.sadd(key2, test3)

        res = yield self.ds.sdiff(key1, key2)
        self.failUnlessEqual(res, s1.difference(s2))

        yield self.ds.remove(key1)
        yield self.ds.remove(key2)

    @defer.inlineCallbacks
    def test_sdiffstore(self):
        key1 = 'key1'
        key2 = 'key2'
        test1 = 'test1'
        test2 = 'test2'
        test3 = 'test3'
        s1 = set([test1, test2])
        yield self.ds.sadd(key1, test1)
        yield self.ds.sadd(key1, test2)

        s2 = set([test2, test3])
        yield self.ds.sadd(key2, test2)
        yield self.ds.sadd(key2, test3)

        dstkey = 'dstkey'
        yield self.ds.sdiffstore(dstkey, key1, key2)
        res = yield self.ds.smembers(dstkey)
        self.failUnlessEqual(res, s1.difference(s2))

        yield self.ds.remove(key1)
        yield self.ds.remove(key2)
        yield self.ds.remove(dstkey)

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


