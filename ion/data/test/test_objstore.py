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


import ion.util.procutils as pu

class BaseObjectTest(unittest.TestCase):

    obj_type_str = None
    obj_cls = objstore.BaseObject


class BlobObjectTest(unittest.TestCase):

    def setUp(self):
        self.blob = objstore.Blob('foo')
        self.encoded = "blob 3\x00foo"

    def test_type(self):
        self.failUnlessEqual(self.blob.type, 'blob')

    def test_hash(self):
        thash = objstore.sha1(self.encoded)
        self.failUnlessEqual(self.blob.hash, thash)

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
        self.failUnlessEqual(self.blob.hash, test.hash)




class TreeObjectTest(unittest.TestCase):

    def setUp(self):
        self.tree = objstore.Tree(
                ('thing', 'd670460b4b4aece5915caf5c68d12f560a9fe3e4', '100644'),
                ('scaleing.py', 'cd9231fa06abb69a380d3f4490a9e261e03beb5a', '100644'))
        self.encoded = """tree 112\x00100644 thing\x00d670460b4b4aece5915caf5c68d12f560a9fe3e4100644 scaleing.py\x00cd9231fa06abb69a380d3f4490a9e261e03beb5a""" 

    def test_type(self):
        self.failUnlessEqual(self.tree.type, 'tree')

    def test_hash(self):
        thash = objstore.sha1(self.encoded)
        self.failUnlessEqual(self.tree.hash, thash)

    def test_encode(self):
        self.failUnlessEqual(self.tree.encode(), self.encoded)

    def test_decode_full(self):
        """encoded is header + raw content body
        """
        test = objstore.Tree.decode_full(self.encoded)
        self.failUnlessEqual(self.tree.hash, test.hash)


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
        thash = objstore.sha1(self.encoded)
        self.failUnlessEqual(self.commit.hash, thash)

    def test_encode(self):
        self.failUnlessEqual(self.commit.encode(), self.encoded)

    def test_decode_full(self):
        test = objstore.Commit.decode_full(self.encoded)
        self.failUnlessEqual(self.commit.hash, test.hash)


class CAStoreTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        backend_store = yield store.Store.create_store()
        self.cas = objstore.CAStore(backend_store)

    @defer.inlineCallbacks
    def test_blob(self):
        c1 = 'test content'
        b =  objstore.Blob(c1)
        yield self.cas.put(b)
        b_out = yield self.cas.get(b.hash)
        self.failUnlessEqual(b.hash, b_out.hash)


class ValueStoreTest(unittest.TestCase):
    """
    Testing value store: store of immutable values (blobs, trees, commits)
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.vs = ValueStore()
        yield self.vs.init()

    def test_ValueObjects(self):
        # Testing all Value Objects
        vref1 = ValueRef('some')
        self.assertEqual(vref1.identity, 'some')
        self.assertEqual(vref1.vtype, None)
        vref2 = ValueRef('other','X')
        self.assertEqual(vref2.vtype, 'X')

        # Blob value objects
        vo1 = ValueObject('1')
        self.assertNotEqual(vo1.identity, None)
        self.assertEqual(vo1.vtype, 'B')
        print vo1
        print "vo1=", vo1.__dict__

        vo2 = ValueObject(2)
        self.assertNotEqual(vo1.identity, vo2.identity)

        vo3 = ValueObject(('a','b'))
        self.assertNotEqual(vo1.identity, vo3.identity)
        vo4 = ValueObject(['a','b'])
        vo5 = ValueObject({'a':'b', 'c':(1,2), 'd':{}, 'e':{'x':'y'}})
        self.assertNotEqual(vo5.identity, vo4.identity)
        print "vo5=", vo5.__dict__

        # Tree value objects with childrefs
        tv0 = TreeValue()
        self.assertEqual(tv0.vtype, 'T')
        tv1 = TreeValue(())
        self.assertEqual(tv0.identity, tv1.identity)

        tv2 = TreeValue(vo1.identity)
        tv3 = TreeValue((vo1.identity,))
        tv4 = TreeValue(vo1)
        tv5 = TreeValue((vo1,))
        self.assertEqual(tv2.identity, tv3.identity)
        self.assertEqual(tv4.identity, tv5.identity)
        self.assertEqual(tv2.identity, tv4.identity)
        self.assertNotEqual(tv0.identity, tv2.identity)

        tv6 = TreeValue((vo2,vo3))
        print "tv6=", tv6.__dict__
        tv7 = TreeValue((vo3,vo2))
        self.assertEqual(tv6.identity, tv7.identity)
        tv8 = TreeValue(tv6)
        tv9 = TreeValue((tv6,tv7,vo5))

        # Check cycle in tree
        tv10 = TreeValue((tv8,tv6))

        # Commit value objects with root tree
        cv0 = CommitValue()
        self.assertEqual(cv0.vtype, 'C')
        cv1 = CommitValue(tv2)
        cv2 = CommitValue(tv2.identity)
        self.assertEqual(cv1.identity, cv2.identity)
        self.assertNotEqual(cv0.identity, cv1.identity)

        # Commit value objects with parent commits
        cv5 = CommitValue(None, (cv1,cv0))
        cv6 = CommitValue(None, cv2)
        cv7 = CommitValue(None, (cv0,cv1))
        self.assertNotEqual(cv5.identity, cv7.identity)

        # Composite value objects with childref and base
        cvc1 = CommitValue(tv2,(cv1,cv2))
        print "cvc1=", cvc1.__dict__

        cvc2 = CommitValue(tv2,cvc1,ts=123,committer='mike',author='some')
        self.assertEqual(cvc2.value['committer'],'mike')
        print "cvc2=", cvc2.__dict__

        # Reference values
        rv1 = RefValue('ref1')
        print "rv1=", rv1.__dict__
        self.assertEqual(rv1.vtype, 'R')
        rv2 = RefValue('ref2')

        # Test ValueRef generation
        vr1 = cvc1._value_ref()
        self.assertEqual(vr1.identity, cvc1.identity)
        self.assertFalse(vr1 is cvc1)
        self.assertFalse(hasattr(vr1,'value'))
        self.assertEqual(vr1.vtype, 'C')

    @defer.inlineCallbacks
    def test_ValueStore(self):
        vo1 = ValueObject('1')
        rvo1 = yield self.vs.put_value(vo1)
        rvo2 = yield self.vs.put_value(vo1)

        # Put in values and trees
        r1 = yield self.vs.put_value('1')
        print "r1=", r1
        self.assertTrue(isinstance(r1, ValueRef))
        self.assertFalse(hasattr(r1, 'value'))
        # Check that a value object actually was placed in the values store
        re1 = yield self.vs.exists_value(ValueObject('1').identity)
        self.assertTrue(re1)
        self.assertEqual(rvo1.identity, r1.identity)

        r2 = yield self.vs.put_value('2')
        rt1 = yield self.vs.put_tree((r1,r2))
        r3 = yield self.vs.put_value('3')
        rt2 = yield self.vs.put_tree((r3,rt1))
        r4 = yield self.vs.put_value('4')
        r5 = yield self.vs.put_value('5')
        rt3 = yield self.vs.put_tree((r4,r5))
        rt4 = yield self.vs.put_tree((rt2,rt3))

        # Test gets
        rv1 = yield self.vs.get_value(r1)
        self.assertEqual(rv1.value, '1')
        self.assertEqual(rv1.vtype, 'B')
        rvs1 = yield self.vs.get_value(r1.identity)
        self.assertEqual(rv1.identity, rvs1.identity)

        rv2 = yield self.vs.get_value(rt1)
        self.assertEqual(rv2.vtype, 'T')
        self.assertEqual(rv2.value['children'][0]['ref'], r1.identity)

        rv3 = yield self.vs.get_value('not_exist')
        self.assertEqual(rv3, None)

        rest1 = yield self.vs.get_tree_entries(rt1)
        print "rest1=", rest1
        self.assertEqual(len(rest1), 2)
        self.assertTrue(rest1[0]['ref'] == r1.identity or rest1[1]['ref'] == r1.identity)
        self.assertTrue(rest1[0]['ref'] == r2.identity or rest1[1]['ref'] == r2.identity)
        rest2 = yield self.vs.get_tree_entries('not_exist')
        self.assertEqual(rest2, None)

        restv1 = yield self.vs.get_tree_entriesvalues(rt1)
        self.assertEqual(len(restv1), 2)
        self.assertEqual(int(restv1[0].value.value)+int(restv1[1].value.value), 3)
        restv2 = yield self.vs.get_tree_entriesvalues('not_exist')
        self.assertEqual(restv2, None)

        # Commits
        rc1 = yield self.vs.put_commit(rt1)
        rc2 = yield self.vs.put_commit(rt2,rc1)
        rc3 = yield self.vs.put_commit(rt2,rc2,committer='me')

        rcg1 = yield self.vs.get_commit(rc1)
        self.assertEqual(rcg1.identity, rc1.identity)
        self.assertEqual(rcg1.value['parents'], [])
        self.assertEqual(rcg1.value['roottree'], rt1.identity)

        rcgv1 = yield self.vs.get_commit_root_entriesvalues(rc1)
        self.assertEqual(len(rcgv1), 2)
        self.assertEqual(int(rcgv1[0].value.value)+int(rcgv1[1].value.value), 3)

class ObjectStoreTest(unittest.TestCase):
    """
    Testing object store
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.os = ObjectStore()
        yield self.os.init()

        self.vo1 = ValueObject('1')
        self.vo2 = ValueObject('2')
        self.vo3 = ValueObject('3')
        self.vo4 = ValueObject('4')
        self.vo5 = ValueObject('5')

        self.tv1 = TreeValue((self.vo1, self.vo2))
        self.tv2 = TreeValue((self.vo3, self.vo4))
        self.tv3 = TreeValue(self.vo5)
        self.tv4 = TreeValue(self.tv1)

        #self.cv0 = CommitValue()
        #self.cv1 = CommitValue(vo5)
        #self.cv2 = CommitValue(vo11)
        #self.cv3 = CommitValue()
        #self.cv4 = CommitValue((vo12,vo13))
        #self.cv5 = CommitValue(vo14)

    def _test_ObjectStore_DataObjs(self):
        # Putting data objects into
        pass

    @defer.inlineCallbacks
    def test_ObjectStore_values(self):
        # Check put
        r1 = yield self.os.put('key1','1')
        print "r1=", r1.__dict__
        self.assertTrue(isinstance(r1, ValueRef))
        self.assertFalse(hasattr(r1, 'value'))
        self.assertEqual(self.os._num_entities(),1)
        self.assertEqual(self.os._num_values(),3)
        # Check that a value object actually was placed in the values store
        re1 = yield self.os.vs.exists_value(ValueObject('1').identity)
        re1 = yield self.os.vs.exists_value(self.vo1.identity)
        self.assertTrue(re1)

        r2 = yield self.os.put('key2','2')
        print "r2=", r2.__dict__

        nume = self.os._num_entities()
        numv = self.os._num_values()
        rt1 = yield self.os.put('tree1-2',(r1,r2))
        self.assertEqual(self.os._num_entities()-nume,1)
        self.assertEqual(self.os._num_values()-numv,2)

        nume = self.os._num_entities()
        numv = self.os._num_values()
        r3 = yield self.os.put('key3',('3',rt1))
        re2 = yield self.os.vs.exists_value(ValueObject('3').identity)
        self.assertTrue(re2)
        self.assertEqual(self.os._num_entities()-nume,1)
        self.assertEqual(self.os._num_values()-numv,3)

        r4 = yield self.os.put('key4','4')
        r5 = yield self.os.put('key5','5')
        rt3 = yield self.os.put('tree4-5',(r4,r5))
        rt4 = yield self.os.put('treet1-t2',(rt3,r3))

        nume = self.os._num_entities()
        numv = self.os._num_values()
        r6 = yield self.os.put('key1','6')
        re3 = yield self.os.vs.exists_value(ValueObject('6').identity)
        self.assertTrue(re3)
        self.assertEqual(self.os._num_entities()-nume,0)
        self.assertEqual(self.os._num_values()-numv,3)

        nume = self.os._num_entities()
        numv = self.os._num_values()
        r7 = yield self.os.put('key1','1')
        self.assertEqual(self.os._num_entities()-nume,0)
        self.assertEqual(self.os._num_values()-numv,1)

        r11 = yield self.os.put('key11','11',r5)
        r12 = yield self.os.put('key12','12',r11)
        r13 = yield self.os.put('key13','13')
        r14 = yield self.os.put('key14','14',(r12,r13))
        r15 = yield self.os.put('key15','15',r14)

        # Check get
        fg0 = yield self.os.get('not_exist')
        self.assertEqual(fg0, None)

        fg1 = yield self.os.get('key1')
        print "fg1=", fg1.__dict__
        self.assertEqual(fg1.get_attr('value'),'1')
        self.assertEqual(fg1.get_attr('value'),self.vo1.value)
        self.assertEqual(fg1.identity,self.vo1.identity)

        fg2 = yield self.os.get('key2')
        self.assertEqual(fg2.identity,self.vo2.identity)

        fg3 = yield self.os.get('tree1-2')
        print "fg3=", fg3.__dict__
        #self.assertEqual(fg3.get_attr(self.vo1.identity).get_attr('value'),'1')
        #self.assertEqual(fg3.get_attr(self.vo2.identity).get_attr('value'),'2')

        #self.assertEqual(len(fg3['basedon']),0)
        #print "vo3=",vo3.state
        #self.assertEqual(fg3['identity'],vo3.identity)
        #
        #fg5 = yield self.os.get('key5')
        #print "fg5=", fg5
        #self.assertIn(vo3.identity,fg5['childrefs'])
        #
        #fg11 = yield self.os.get('key11'))
        #self.assertEqual(len(fg11['childrefs']),0)
        #self.assertIn(vo5.identity,fg11['basedon'])
        #
        #fg14 = yield self.os.get('key14'))
        #self.assertEqual(len(fg14['basedon']),2)
        #self.assertIn(vo12.identity,fg14['basedon'])
        #self.assertIn(vo13.identity,fg14['basedon'])
        #
        ## Check get_values
        #rv1 = _opfix('get_values',_cont([r1['identity'],r2['identity'],r4['identity'],r5['identity']]))
        #print "rv1=", rv1
        #self.assertEqual(len(rv1),4)
        #self.assertEqual(r1['identity'],rv1[0]['identity'])
        #self.assertEqual(r2['identity'],rv1[1]['identity'])
        #self.assertEqual(r4['identity'],rv1[2]['identity'])
        #self.assertEqual(r5['identity'],rv1[3]['identity'])
        #
        ## Check get_ancestors
        #ra1 = _opfix('get_ancestors',_cont(r1['identity']))
        #print "ra1=", ra1
        #self.assertEqual(len(ra1),0)
        #
        #ra5 = _opfix('get_ancestors',_cont(r5['identity']))
        #print "ra5=", ra5
        #self.assertEqual(len(ra5),0)
        #
        #ra15 = _opfix('get_ancestors',_cont(r15['identity']))
        #print "ra15=", ra15
        #self.assertEqual(len(ra15),5)




