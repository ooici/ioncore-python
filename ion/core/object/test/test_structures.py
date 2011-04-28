#!/usr/bin/env python
"""
@brief Test possible data structures

@file ion/core/object/test/test_structures.py
@author David Stuebe
@test Testing the data object management framework for complex structures
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from twisted.trial import unittest

from ion.test.iontest import IonTestCase

from ion.core.object import workbench
from ion.core.object import object_utils
from ion.core.object import gpb_wrapper


#basic_pb2.KeyValue
keyvalue_type = object_utils.create_type_identifier(object_id=20005, version=1)

#basic_pb2.TestObj
testobj_type = object_utils.create_type_identifier(object_id=20010, version=1)

#basic_pb2.ListObj
listobj_type = object_utils.create_type_identifier(object_id=20004, version=1)

class IntegerAndFloatTest(unittest.TestCase):

    def test_f_nans(self):

        simple1 = gpb_wrapper.Wrapper._create_object(testobj_type)
        simple1.float = float('nan')

        simple2 = gpb_wrapper.Wrapper._create_object(testobj_type)
        simple2.float = float('nan')

        # Nans are not equal in Python
        self.failIf(simple1==simple2)       # False
        self.failUnless(simple1>simple2)    # True
        self.failUnless(simple1>=simple2)   # True
        self.failIf(simple1<=simple2)       # False
        self.failIf(simple1<simple2)        # False

    def test_f_inf(self):

        simple1 = gpb_wrapper.Wrapper._create_object(testobj_type)
        simple1.float = float('inf')

        simple2 = gpb_wrapper.Wrapper._create_object(testobj_type)
        simple2.float = float('inf')

        # Nans are not equal in Python
        self.assertEqual(simple1,simple2)

    def test_f_ninf(self):

        simple1 = gpb_wrapper.Wrapper._create_object(testobj_type)
        simple1.float = float('-inf')

        simple2 = gpb_wrapper.Wrapper._create_object(testobj_type)
        simple2.float = float('-inf')

        # Nans are not equal in Python
        self.assertEqual(simple1,simple2)


    def test_i(self):

        simple1 = gpb_wrapper.Wrapper._create_object(testobj_type)
        simple1.integer = 1

        simple2 = gpb_wrapper.Wrapper._create_object(testobj_type)
        simple2.integer = 1

        # Nans are not equal in Python
        self.assertEqual(simple1,simple2)



class SimpleObjectTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        
        repo, simple = wb.init_repository(testobj_type)

        simple.string = 'abc'
        simple.integer = 5
        simple.float = 3.

        simple.strings.extend(['stuff','junk','more'])
        simple.integers.extend([1,2,3,4])
        simple.floats.extend([3.,4.,5.,6.])
        
        repo.commit('First Commit')
        self.wb = wb
        self.repo = repo
        
    @defer.inlineCallbacks
    def test_init(self):
        simple = yield self.repo.checkout('master')
        
        self.assertEqual(len(simple.ParentLinks),1)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, False)
        
    @defer.inlineCallbacks
    def test_modify_string(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.string, 'abc')
        simple.string = 'xyz'
        self.assertEqual(simple.string, 'xyz')
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    @defer.inlineCallbacks
    def test_modify_integer(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.integer, 5)

        simple.integer = 50
        self.assertEqual(simple.integer, 50)

        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    @defer.inlineCallbacks
    def test_modify_float(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.float, 3.)
        
        simple.float = 50
        self.assertEqual(simple.float, 50.)
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    @defer.inlineCallbacks
    def test_modify_strings(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.strings, ['stuff','junk','more'])
        
        simple.strings.insert(2,'xyz')
        self.assertEqual(simple.strings, ['stuff','junk','xyz','more'])
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    @defer.inlineCallbacks
    def test_modify_integers(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.integers, [1,2,3,4])
        simple.integers.append( 50)
        self.assertEqual(simple.integers, [1,2,3,4,50])
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    @defer.inlineCallbacks
    def test_modify_floats(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        
        simple.floats.remove( 4.0)
        self.assertEqual(simple.floats, [3.,5.,6.])
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    @defer.inlineCallbacks
    def test_setitem(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        simple.floats[1]=( 44.0)
        self.assertEqual(simple.floats, [3.,44.,5.,6.])
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    @defer.inlineCallbacks
    def test_getitem(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        self.assertEqual(simple.floats[1], 4.)
        
        # Should not modify anything!
        self.assertEqual(len(simple.ParentLinks),1)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, False)
        
    @defer.inlineCallbacks
    def test_delitem(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        del simple.floats[1]
        self.assertEqual(simple.floats, [3.,5.,6.])
        
        
        # Should not modify anything!
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
    
    @defer.inlineCallbacks
    def test_delslice(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        del simple.floats[1:3]
        self.assertEqual(simple.floats, [3.,6.])
        
        
        # Should not modify anything!
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    @defer.inlineCallbacks
    def test_invalidate_repateadscalar(self):
        
        simple = yield self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        floats = simple.floats
        
        # Checkout to invalidate it
        simple_2 = yield self.repo.checkout('master')

        self.assertEqual(object.__getattribute__(simple,'Invalid'), True)
        self.assertEqual(object.__getattribute__(floats,'Invalid'), True)
        

class ContainerTest(unittest.TestCase):
        
    def setUp(self):
        self.wb = workbench.WorkBench('No Process Test')
        
        self.repo, self.listobj = self.wb.init_repository(listobj_type)
        
        
    def test_factory(self):
        
        items1 = self.listobj.items
        items2 = self.listobj.items
        
        self.assertIdentical(items1, items2)
        
        self.assertIdentical(items1._wrapper, self.listobj)
        self.assertIdentical(items1.Root, self.listobj.Root)

    @defer.inlineCallbacks
    def test_invalidation(self):
        """
        Test the container method to set link by index
        """
        kv1 = self.repo.create_object(keyvalue_type)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        
        items = self.listobj.items
        
        item1 = items.add()
        items.SetLink(0,kv1)
        
        self.repo.commit('test1')
        
        listobj = yield self.repo.checkout('master')
        
        # Test some internals of the container/checkout functionality
        self.assertEqual(items.Invalid, True)
        self.assertEqual(self.listobj.Invalid, True)
        self.assertEqual(kv1.Invalid, True)
    
    @defer.inlineCallbacks
    def test_add(self):
        """
        Test the container and set the link using the object wrapper
        """
        kv1 = self.repo.create_object(keyvalue_type)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        
        items = self.listobj.items
        
        item1 = items.add()
        item1.SetLink(kv1)
        
        self.assertEqual(kv1.IsRoot, True)
        self.assertNotIdentical(kv1.Root, items.Root)
        
        self.assertEqual(self.listobj.items[0].key, 'foo1')
        
        self.repo.commit('test1')
        
        listobj = yield self.repo.checkout('master')
        
        self.assertEqual(listobj.items[0].value, 'bar1')
        
    @defer.inlineCallbacks        
    def test_setlink(self):
        """
        Test the container method to set link by index
        """
        kv1 = self.repo.create_object(keyvalue_type)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        
        items = self.listobj.items
        
        item1 = items.add()
        items.SetLink(0,kv1)
        
        # Test before commit to make sure the set/get works
        self.assertEqual(self.listobj.items[0].key, 'foo1')
        
        self.repo.commit('test_setlink')
        
        listobj = yield self.repo.checkout('master')
        
        self.assertEqual(listobj.items[0].value, 'bar1')
        
    @defer.inlineCallbacks
    def test_getlink(self):
        """
        Test the container method GetLink() and check link consistency
        Note: Test depends on test_setlink()
        """
        kv1 = self.repo.create_object(keyvalue_type)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        
        items = self.listobj.items
        
        items.add()
        items.SetLink(0, kv1)
        
        # Test before commit to make sure GetLink() works locally
        link1 = items.GetLink(0)
        realized_kv1 = items.Repository.get_linked_object(link1)
        self.assertEqual(realized_kv1.key, 'foo1')
        
        # Test after commit
        self.repo.commit('test_getlink')
        listobj = yield self.repo.checkout('master')
        items = listobj.items
        
        link2 = items.GetLink(0)
        realized_kv2 = items.Repository.get_linked_object(link2)
        self.assertEqual(realized_kv2.value, 'bar1')
        
    @defer.inlineCallbacks
    def test_getlinks(self):
        """
        Test the container method GetLinks() and check link consistency
        Note: Test depends on test_setlink()
        """
        kv1 = self.repo.create_object(keyvalue_type)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        kv2 = self.repo.create_object(keyvalue_type)
        kv2.key = 'foo2'
        kv2.value = 'bar2'
        
        items = self.listobj.items
        
        item1 = items.add()
        item2 = items.add()
        item1.SetLink(kv1)
        item2.SetLink(kv2)
        
        # Test before commit to make sure GetLinks() works locally
        links_list1 = items.GetLinks()
        self.assertTrue(len(links_list1) == 2)
        realized_kv1 = items.Repository.get_linked_object(links_list1[0])
        realized_kv2 = items.Repository.get_linked_object(links_list1[1])
        self.assertEqual(realized_kv1.key, 'foo1')
        self.assertEqual(realized_kv2.key, 'foo2')
        
        # Test after commit
        self.repo.commit('test_getlinks')
        listobj = yield self.repo.checkout('master')
        items = listobj.items
        
        links_list2 = items.GetLinks()
        self.assertTrue(len(links_list2) == 2)
        realized_kv3 = items.Repository.get_linked_object(links_list2[0])
        realized_kv4 = items.Repository.get_linked_object(links_list2[1])
        self.assertEqual(realized_kv3.value, 'bar1')
        self.assertEqual(realized_kv4.value, 'bar2')
        
    @defer.inlineCallbacks
    def test_getitem(self):
        """
        Test the private container method __getitem_() by a given index
        Note: Test depends on test_setlink()
        """
        kv1 = self.repo.create_object(keyvalue_type)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        
        items = self.listobj.items
        
        items.add()
        items.SetLink(0, kv1)
        
        # Test before commit to make sure __getitem__() works locally
        item1 = items[0]
        self.assertEqual(item1.key, 'foo1')
        
        # Test after commit
        self.repo.commit('test_getitem')
        listobj = yield self.repo.checkout('master')
        items = listobj.items
        
        item2 = items[0]
        self.assertEqual(item2.value, 'bar1')
        
    @defer.inlineCallbacks
    def test_getslice(self):
        """
        Test the container method __getslice__() by start and stop indices
        Note: Test depends on test_setlink()
        """
        kv1 = self.repo.create_object(keyvalue_type)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        kv2 = self.repo.create_object(keyvalue_type)
        kv2.key = 'foo2'
        kv2.value = 'bar2'
        kv3 = self.repo.create_object(keyvalue_type)
        kv3.key = 'foo3'
        kv3.value = 'bar3'
        
        items = self.listobj.items
        
        item1 = items.add()
        item2 = items.add()
        item3 = items.add()
        item1.SetLink(kv1)
        item2.SetLink(kv2)
        item3.SetLink(kv3)
        
        # Test before commit to make sure GetLinks() works locally
        items_list = items.__getslice__(0, 2)
        self.assertTrue(len(items_list) == 2)
        self.assertEqual(items_list[0].key, 'foo1')
        self.assertEqual(items_list[1].key, 'foo2')
        
        items_list = items[1:2]
        self.assertTrue(len(items_list) == 1)
        self.assertEqual(items_list[0].key, 'foo2')
        
        items_list = items.__getslice__(1, 5)
        self.assertTrue(len(items_list) == 2)
        self.assertEqual(items_list[0].key, 'foo2')
        self.assertEqual(items_list[1].key, 'foo3')
        
        items_list = items.__getslice__(-2, 3)
        self.assertTrue(len(items_list) == 2)
        self.assertEqual(items_list[0].key, 'foo2')
        self.assertEqual(items_list[1].key, 'foo3')
        
        items_list = items.__getslice__(-5, 1)
        self.assertTrue(len(items_list) == 1)
        self.assertEqual(items_list[0].key, 'foo1')

        items_list = items.__getslice__(-3, -1)
        self.assertTrue(len(items_list) == 2)
        self.assertEqual(items_list[0].key, 'foo1')
        self.assertEqual(items_list[1].key, 'foo2')
        
        
        # Test after commit
        self.repo.commit('test_getslice')
        listobj = yield self.repo.checkout('master')
        items = listobj.items
        
        items_list = items.__getslice__(0, 2)
        self.assertTrue(len(items_list) == 2)
        self.assertEqual(items_list[0].value, 'bar1')
        self.assertEqual(items_list[1].value, 'bar2')
        
        items_list = items.__getslice__(1, 2)
        self.assertTrue(len(items_list) == 1)
        self.assertEqual(items_list[0].value, 'bar2')
        
        items_list = items.__getslice__(1, 5)
        self.assertTrue(len(items_list) == 2)
        self.assertEqual(items_list[0].value, 'bar2')
        self.assertEqual(items_list[1].value, 'bar3')
        
        items_list = items.__getslice__(-2, 3)
        self.assertTrue(len(items_list) == 2)
        self.assertEqual(items_list[0].value, 'bar2')
        self.assertEqual(items_list[1].value, 'bar3')
        
        items_list = items.__getslice__(-5, 1)
        self.assertTrue(len(items_list) == 1)
        self.assertEqual(items_list[0].value, 'bar1')

        items_list = items.__getslice__(-3, -1)
        self.assertTrue(len(items_list) == 2)
        self.assertEqual(items_list[0].value, 'bar1')
        self.assertEqual(items_list[1].value, 'bar2')

    @defer.inlineCallbacks
    def test_del(self):
        """
        Test the container method __delitem__() by index
        Note: Test depends on test_setlink()
        """
        kv1 = self.repo.create_object(keyvalue_type)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        kv2 = self.repo.create_object(keyvalue_type)
        kv2.key = 'foo2'
        kv2.value = 'bar2'
        kv3 = self.repo.create_object(keyvalue_type)
        kv3.key = 'foo3'
        kv3.value = 'bar3'
        
        items = self.listobj.items
        
        item1 = items.add()
        item2 = items.add()
        item3 = items.add()
        item1.SetLink(kv1)
        item2.SetLink(kv2)
        item3.SetLink(kv3)
        
        # Test before commit to make sure __delitem__() works locally
        links_list = items.GetLinks()
        self.assertTrue(len(links_list) == 3)
        items.__delitem__(0)
        links_list = items.GetLinks()
        self.assertTrue(len(links_list) == 2)
        realized_kv1 = items.Repository.get_linked_object(links_list[0])
        realized_kv2 = items.Repository.get_linked_object(links_list[1])
        self.assertEqual(realized_kv1.key, 'foo2')
        self.assertEqual(realized_kv2.key, 'foo3')
        
        # Test after commit
        self.repo.commit('test_del')
        listobj = yield self.repo.checkout('master')
        items = listobj.items
        
        links_list = items.GetLinks()
        self.assertTrue(len(links_list) == 2)
        items.__delitem__(1)
        links_list = items.GetLinks()
        self.assertTrue(len(links_list) == 1)
        realized_kv3 = items.Repository.get_linked_object(links_list[0])
        self.assertEqual(realized_kv3.value, 'bar2')
        
    @defer.inlineCallbacks
    def test_delslice(self):
        """
        Test the container method __delslice__() by start and stop indices
        Note: Test depends on test_setlink() and test_getslice()
        """
        items = self.listobj.items
        
        for idx in range(1, 21):
            kv = self.repo.create_object(keyvalue_type)
            kv.key = 'foo' + str(idx)
            kv.value = 'bar' + str(idx)
            item = items.add()
            item.SetLink(kv)
            
        # Test before commit to make sure __delslice__() works locally for various slices
        items.__delslice__(0, 2)
        items_list = items.__getslice__(0,20)
        self.assertTrue(len(items_list) == 18)
        self.assertEqual(items_list[0].key, 'foo3')
        self.assertEqual(items_list[1].key, 'foo4')
        self.assertEqual(items_list[2].key, 'foo5')
        self.assertEqual(items_list[3].key, 'foo6')
        self.assertEqual(items_list[4].key, 'foo7')
        
        items.__delslice__(1, 2)
        items_list = items.__getslice__(0,20)
        self.assertTrue(len(items_list) == 17)
        self.assertEqual(items_list[0].key, 'foo3')
        self.assertEqual(items_list[1].key, 'foo5')
        self.assertEqual(items_list[2].key, 'foo6')
        self.assertEqual(items_list[3].key, 'foo7')
        self.assertEqual(items_list[4].key, 'foo8')
        
        items.__delslice__(15, 20)
        items_list = items.__getslice__(0,20)
        self.assertTrue(len(items_list) == 15)
        self.assertEqual(items_list[10].key, 'foo14')
        self.assertEqual(items_list[11].key, 'foo15')
        self.assertEqual(items_list[12].key, 'foo16')
        self.assertEqual(items_list[13].key, 'foo17')
        self.assertEqual(items_list[14].key, 'foo18')
        
        items.__delslice__(-12, 5)
        items_list = items.__getslice__(0,20)
        self.assertTrue(len(items_list) == 13)
        self.assertEqual(items_list[0].key, 'foo3')
        self.assertEqual(items_list[1].key, 'foo5')
        self.assertEqual(items_list[2].key, 'foo6')
        self.assertEqual(items_list[3].key, 'foo9')
        self.assertEqual(items_list[4].key, 'foo10')
        
        items.__delslice__(-20, 1)
        items_list = items.__getslice__(0,20)
        self.assertTrue(len(items_list) == 12)
        self.assertEqual(items_list[0].key, 'foo5')
        self.assertEqual(items_list[1].key, 'foo6')
        self.assertEqual(items_list[2].key, 'foo9')
        self.assertEqual(items_list[3].key, 'foo10')
        self.assertEqual(items_list[4].key, 'foo11')

        items.__delslice__(-9, -8)
        items_list = items.__getslice__(0,20)
        self.assertTrue(len(items_list) == 11)
        self.assertEqual(items_list[0].key, 'foo5')
        self.assertEqual(items_list[1].key, 'foo6')
        self.assertEqual(items_list[2].key, 'foo9')
        self.assertEqual(items_list[3].key, 'foo11')
        self.assertEqual(items_list[4].key, 'foo12')
        
        # Test if deletes carry over during commit
        self.repo.commit('test_getslice1')
        listobj = yield self.repo.checkout('master')
        items = listobj.items
        
        items_list = items.__getslice__(0, 20)
        self.assertTrue(len(items_list) == 11)
        self.assertEqual(items_list[0].key, 'foo5')
        self.assertEqual(items_list[1].key, 'foo6')
        self.assertEqual(items_list[2].key, 'foo9')
        self.assertEqual(items_list[3].key, 'foo11')
        self.assertEqual(items_list[4].key, 'foo12')
        self.assertEqual(items_list[5].key, 'foo13')
        self.assertEqual(items_list[6].key, 'foo14')
        self.assertEqual(items_list[7].key, 'foo15')
        self.assertEqual(items_list[8].key, 'foo16')
        self.assertEqual(items_list[9].key, 'foo17')
        self.assertEqual(items_list[10].key, 'foo18')
        
        # One more check by commit...
        items.__delslice__(5, 9)
        self.repo.commit('test_getslice2')
        listobj = yield self.repo.checkout('master')
        items = listobj.items
        
        items_list = items.__getslice__(0, 20)
        self.assertTrue(len(items_list) == 7)
        self.assertEqual(items_list[0].key, 'foo5')
        self.assertEqual(items_list[1].key, 'foo6')
        self.assertEqual(items_list[2].key, 'foo9')
        self.assertEqual(items_list[3].key, 'foo11')
        self.assertEqual(items_list[4].key, 'foo12')
        self.assertEqual(items_list[5].key, 'foo17')
        self.assertEqual(items_list[6].key, 'foo18')
        
        
    #def testsetcontainerlink(self):
    #    kv1 = self.repo.create_object(keyvalue_type)
    #    kv1.key = 'foo1'
    #    kv1.value = 'bar1'
    #    
    #    items = self.listobj.items
    #    
    #    item1 = items.add()
    #    items[0] = kv1
    #    
    #    self.assertEqual(self.listobj.items[0].key, 'foo1')
    #    
    #    self.repo.commit('test1')
    #    
    #    listobj = yield self.repo.checkout('master')
    #    
    #    
        #
        #
        #
        #kv2 = repo.create_object(keyvalue_type)
        #kv2.key = 'foo2'
        #kv2.value = 'bar2'
        #
        #kv3 = repo.create_object(keyvalue_type)
        #kv3.key = 'foo3'
        #kv3.value = 'bar3'
        #
        #
        #
        #
        #listobject.items.add()
        #lisgobject.items[1] = kv2
        #
        #
        #repo.commit('First Commit')
        #self.wb = wb
        #self.repo = repo
        #
        #
        #
        