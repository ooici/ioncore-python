#!/usr/bin/env python
"""
@file ion/data/datastore/test/test_cas.py
@author Michael Meisinger
@author Dorian Raymer
@brief test object store
"""

import logging
log = logging.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest

from ion.data import store
from ion.data.datastore import cas

sha1 = cas.sha1

class BlobObjectTest(unittest.TestCase):

    def setUp(self):
        self.blob = cas.Blob('foo')
        self.encoded = "blob 3\x00foo"

    def test_type(self):
        self.failUnlessEqual(self.blob.type, 'blob')

    def test_hash(self):
        thash = sha1(self.encoded)
        self.failUnlessEqual(sha1(self.blob), thash)

    def test_encode(self):
        b = cas.Blob('foo')
        self.failUnlessEqual(
                b.encode(),
                self.encoded
                )

    def test_decode_full(self):
        """encoded is header + raw content body
        """
        test = cas.Blob.decode_full(self.encoded)
        self.failUnlessEqual(sha1(self.blob.value), sha1(test.value))

    def test_blob_str(self):
        string = str(self.blob)
        test = '\n========== Store Type: blob ==========\n'
        test += '= Key: "19102815663d23f8b75a47e7a01965dcdc96468c"\n'
        test += '= Content: "foo"\n'
        test += '='*20
        self.assertEqual(string,test)

class TreeObjectTest(unittest.TestCase):

    def setUp(self):
        self.tree = cas.Tree(
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
        test = cas.Tree.decode_full(self.encoded)
        self.failUnlessEqual(sha1(self.tree), sha1(test))

    def test_tree_print(self):
        string = str(self.tree)
        test = '\n========== Store Type: tree ==========\n'
        test += '= Key: "b47a541cee0f0b8fd8a5af0fad8821de87772b1e"\n'
        test += '= name: "thing", id: "08cf6101416f0ce0dda3c80e627f333854c4085c"\n'
        test += '= name: "scaleing.py", id: "cd9231fa06abb69a380d3f4490a9e261e03beb5a"\n'
        test += '='*20
        self.assertEqual(string,test)

class CommitObjectTest(unittest.TestCase):

    def setUp(self):
        self.tree = '80655da8d80aaaf92ce5357e7828dc09adb00993'
        self.parent = 'd8fd39d0bbdd2dcf322d8b11390a4c5825b11495'
        self.parent2 = '28fd39d0bbdd2dcf322d8b11390a4c5825b11495'
        self.commit = cas.Commit(self.tree, [self.parent], log="foo bar")
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
        test = cas.Commit.decode_full(self.encoded)
        self.failUnlessEqual(sha1(self.commit), sha1(test))
    
    def test_commit_print(self):
        string = str(self.commit)
        test = '\n========== Store Type: commit ==========\n'
        test += '= Key: "252f7b9b624170607c13e9370a560d75d0a9b9ea"\n'
        test += '= Tree: "80655da8d80aaaf92ce5357e7828dc09adb00993"\n'
        test += '= Parent: "d8fd39d0bbdd2dcf322d8b11390a4c5825b11495"\n' 
        test += '= Log: "foo bar"\n'
        test += '='*20
        self.assertEqual(string,test)


class CAStoreTest(unittest.TestCase):
    """@brief Test of basic fundamental capabilities of the CA Store and
    the store objects.
    """

    @defer.inlineCallbacks
    def setUp(self):
        """
        Test the store mechanics with the in-memory Store backend.
        """
        backend_store = yield store.Store.create_store()
        self.cas = cas.CAStore(backend_store)

    @defer.inlineCallbacks
    def test_blob(self):
        c1 = 'test content'
        b =  cas.Blob(c1)
        bid = yield self.cas.put(b)
        b_out = yield self.cas.get(bid)
        self.failUnlessEqual(sha1(b), sha1(b_out))

    @defer.inlineCallbacks
    def test_tree(self):
        b =  cas.Blob('test content')
        yield self.cas.put(b)
        t1 = cas.Tree(cas.Element('test', sha1(b)))
        t1id = yield self.cas.put(t1)
        t1_out = yield self.cas.get(t1id)
        self.failUnlessEqual(t1_out.value, t1.value)

    @defer.inlineCallbacks
    def test_tree2(self):
        b =  cas.Blob('test content')
        b2 =  cas.Blob('deja vu')
        b3 =  cas.Blob('jamais vu')
        bid = yield self.cas.put(b)
        b2id = yield self.cas.put(b2)
        b3id = yield self.cas.put(b3)
        t1 = cas.Tree(cas.Element('test', sha1(b)),
                            cas.Element('hello', sha1(b2)))
        t1id = yield self.cas.put(t1)
        t2 = cas.Tree(cas.Element('thing', sha1(b3)),
                            cas.Element('tree', sha1(t1)))
        t2id = yield self.cas.put(t2)
        t1_out = yield self.cas.get(t1id)
        self.failUnlessEqual(t1_out.value, t1.value)
        t2_out = yield self.cas.get(t2id)
        self.failUnlessEqual(t2_out.value, t2.value)

    @defer.inlineCallbacks
    def test_commit(self):
        b =  cas.Blob('test content')
        b2 =  cas.Blob('deja vu')
        b3 =  cas.Blob('jamais vu')
        yield self.cas.put(b)
        yield self.cas.put(b2)
        yield self.cas.put(b3)

        t1 = cas.Tree(cas.Element('test', sha1(b)),
                            cas.Element('hello', sha1(b2)))
        t1id = yield self.cas.put(t1)
        t2 = cas.Tree(cas.Element('thing', sha1(b3)),
                            cas.Element('tree', sha1(t1)))
        t2id = yield self.cas.put(t2)
        c = cas.Commit(t2id, log='first commit')
        cid = yield self.cas.put(c)
        c_out = yield self.cas.get(cid)
        self.failUnlessEqual(c.value, c_out.value)

    @defer.inlineCallbacks
    def test_commit2(self):
        b =  cas.Blob('test content')
        b2 =  cas.Blob('deja vu')
        b3 =  cas.Blob('jamais vu')
        yield self.cas.put(b)
        yield self.cas.put(b2)
        yield self.cas.put(b3)
        t1 = cas.Tree(cas.Element('test', sha1(b)),
                            cas.Element('hello', sha1(b2)))
        t1id = yield self.cas.put(t1)
        t2 = cas.Tree(cas.Element('thing', sha1(b3)),
                            cas.Element('tree', sha1(t1)))
        t2id = yield self.cas.put(t2)
        c = cas.Commit(t2id, log='first commit')
        cid = yield self.cas.put(c)
        c_out = yield self.cas.get(cid)
        b3new = cas.Blob('I remember, now!')
        b3newid = yield self.cas.put(b3new)

        t2new = cas.Tree(cas.Element('thing', sha1(b3new)),
                            cas.Element('tree', sha1(t1)))
        t2newid = yield self.cas.put(t2new)
        cnew = cas.Commit(t2newid, parents=[cid], log='know what i knew but forgot')
        cnewid = yield self.cas.put(cnew)
        cnew_out = yield self.cas.get(cnewid)
        self.failUnlessEqual(cnew.value, cnew_out.value)

    @defer.inlineCallbacks
    def test_not_found(self):
        try:
            obj = yield self.cas.get(sha1('not there'))
            self.fail()
        except cas.CAStoreError:
            pass



