#!/usr/bin/env python

"""
@file ion/data/test/test_objstore.py
@author Michael Meisinger
@author Dorian Raymer
@brief test object store
"""

import logging

from twisted.internet import defer
from twisted.trial import unittest

from ion.core import base_process, bootstrap

from ion.data.objstore import ValueObject, TreeValue, CommitValue, RefValue, ValueRef
from ion.data.objstore import ObjectStore, ValueStore

from ion.data import store
from ion.data import objstore
from ion.data.objstore import sha1


import ion.util.procutils as pu

class BlobObjectTest(unittest.TestCase):

    def setUp(self):
        self.blob = objstore.Blob('foo')
        self.encoded = "blob 3\x00foo"

    def test_type(self):
        self.failUnlessEqual(self.blob.type, 'blob')

    def test_hash(self):
        thash = sha1(self.encoded)
        self.failUnlessEqual(sha1(self.blob), thash)

    def test_encode(self):
        b = objstore.Blob('foo')
        self.failUnlessEqual(
                b.encode(),
                self.encoded
                )

    def test_decode_full(self):
        """encoded is header + raw content body
        """
        test = objstore.Blob.decode_full(self.encoded)
        self.failUnlessEqual(sha1(self.blob.value), sha1(test.value))

class TreeObjectTest(unittest.TestCase):

    def setUp(self):
        self.tree = objstore.Tree(
                ('thing', '\x08\xcfa\x01Ao\x0c\xe0\xdd\xa3\xc8\x0eb\x7f38T\xc4\x08\\', '100644'),
                ('scaleing.py', '\xcd\x921\xfa\x06\xab\xb6\x9a8\r?D\x90\xa9\xe2a\xe0;\xebZ', '100644'))
        self.encoded = """tree 72\x00100644 thing\x00\x08\xcfa\x01Ao\x0c\xe0\xdd\xa3\xc8\x0eb\x7f38T\xc4\x08\\100644 scaleing.py\x00\xcd\x921\xfa\x06\xab\xb6\x9a8\r?D\x90\xa9\xe2a\xe0;\xebZ""" 

    def test_type(self):
        self.failUnlessEqual(self.tree.type, 'tree')

    def test_hash(self):
        thash = sha1(self.encoded)
        self.failUnlessEqual(sha1(self.tree), thash)

    def test_encode(self):
        self.failUnlessEqual(self.tree.encode(), self.encoded)

    def test_decode_full(self):
        """encoded is header + raw content body
        """
        test = objstore.Tree.decode_full(self.encoded)
        self.failUnlessEqual(sha1(self.tree), sha1(test))

class CommitObjectTest(unittest.TestCase):

    def setUp(self):
        self.tree = '80655da8d80aaaf92ce5357e7828dc09adb00993'
        self.parent = 'd8fd39d0bbdd2dcf322d8b11390a4c5825b11495'
        self.parent2 = '28fd39d0bbdd2dcf322d8b11390a4c5825b11495'
        self.commit = objstore.Commit(self.tree, [self.parent], log="foo bar")
        _body = "tree %s\nparent %s\n\n%s" % (self.tree, self.parent, "foo bar",)
        self.encoded = "commit %d\x00%s" % (len(_body), _body,)

    def test_type(self):
        self.failUnlessEqual(self.commit.type, 'commit')

    def test_hash(self):
        thash = sha1(self.encoded)
        self.failUnlessEqual(sha1(self.commit), thash)

    def test_encode(self):
        self.failUnlessEqual(self.commit.encode(), self.encoded)

    def test_decode_full(self):
        test = objstore.Commit.decode_full(self.encoded)
        self.failUnlessEqual(sha1(self.commit), sha1(test))


class CAStoreTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        """
        Test the store mechanics with the in-memory Store backend.
        """
        backend_store = yield store.Store.create_store()
        self.cas = objstore.CAStore(backend_store)

    @defer.inlineCallbacks
    def test_blob(self):
        c1 = 'test content'
        b =  objstore.Blob(c1)
        yield self.cas.put(b)
        b_out = yield self.cas.get(b.hash)
        self.failUnlessEqual(b.hash, b_out.hash)

    @defer.inlineCallbacks
    def test_tree(self):
        b =  objstore.Blob('test content')
        yield self.cas.put(b)
        t1 = objstore.Tree(objstore.Entity('test', sha1(b)))
        t1id = yield self.cas.put(t1)
        t1_out = yield self.cas.get(t1id)
        self.failUnlessEqual(t1_out.value, t1.value)

    @defer.inlineCallbacks
    def test_tree2(self):
        b =  objstore.Blob('test content')
        b2 =  objstore.Blob('deja vu')
        b3 =  objstore.Blob('jamais vu')
        bid = yield self.cas.put(b)
        b2id = yield self.cas.put(b2)
        b3id = yield self.cas.put(b3)
        t1 = objstore.Tree(objstore.Entity('test', sha1(b)),
                            objstore.Entity('hello', sha1(b2)))
        t1id = yield self.cas.put(t1)
        t2 = objstore.Tree(objstore.Entity('thing', sha1(b3)),
                            objstore.Entity('tree', sha1(t1)))
        t2id = yield self.cas.put(t2)
        t1_out = yield self.cas.get(t1id)
        self.failUnlessEqual(t1_out.value, t1.value)
        t2_out = yield self.cas.get(t2id)
        self.failUnlessEqual(t2_out.value, t2.value)

    @defer.inlineCallbacks
    def test_commit(self):
        b =  objstore.Blob('test content')
        b2 =  objstore.Blob('deja vu')
        b3 =  objstore.Blob('jamais vu')
        yield self.cas.put(b)
        yield self.cas.put(b2)
        yield self.cas.put(b3)

        t1 = objstore.Tree(objstore.Entity('test', sha1(b)),
                            objstore.Entity('hello', sha1(b2)))
        t1id = yield self.cas.put(t1)
        t2 = objstore.Tree(objstore.Entity('thing', sha1(b3)),
                            objstore.Entity('tree', sha1(t1)))
        t2id = yield self.cas.put(t2)
        c = objstore.Commit(t2id, log='first commit')
        cid = yield self.cas.put(c)
        c_out = yield self.cas.get(cid)
        self.failUnlessEqual(c.value, c_out.value)

    @defer.inlineCallbacks
    def test_commit2(self):
        b =  objstore.Blob('test content')
        b2 =  objstore.Blob('deja vu')
        b3 =  objstore.Blob('jamais vu')
        yield self.cas.put(b)
        yield self.cas.put(b2)
        yield self.cas.put(b3)
        t1 = objstore.Tree(objstore.Entity('test', sha1(b)),
                            objstore.Entity('hello', sha1(b2)))
        t1id = yield self.cas.put(t1)
        t2 = objstore.Tree(objstore.Entity('thing', sha1(b3)),
                            objstore.Entity('tree', sha1(t1)))
        t2id = yield self.cas.put(t2)
        c = objstore.Commit(t2id, log='first commit')
        cid = yield self.cas.put(c)
        c_out = yield self.cas.get(cid)
        b3new = objstore.Blob('I remember, now!')
        b3newid = yield self.cas.put(b3new)

        t2new = objstore.Tree(objstore.Entity('thing', sha1(b3new)),
                            objstore.Entity('tree', sha1(t1)))
        t2newid = yield self.cas.put(t2new)
        cnew = objstore.Commit(t2newid, parents=[cid], log='know what i knew but forgot')
        cnewid = yield self.cas.put(cnew)
        cnew_out = yield self.cas.get(cnewid)
        self.failUnlessEqual(cnew.value, cnew_out.value)





