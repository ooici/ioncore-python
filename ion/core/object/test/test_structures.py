#!/usr/bin/env python
"""
@brief Test possible data structures

@file ion/core/object/test/test_structures.py
@author David Stuebe
@test Testing the data object management framework for complex structures
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


from twisted.trial import unittest

from ion.test.iontest import IonTestCase

from net.ooici.basic import basic_pb2
from ion.core.object import workbench


class SimpleObjectTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        
        repo, simple = wb.init_repository(basic_pb2.TestObj)

        simple.string = 'abc'
        simple.integer = 5
        simple.float = 3.

        simple.strings.extend(['stuff','junk','more'])
        simple.integers.extend([1,2,3,4])
        simple.floats.extend([3.,4.,5.,6.])
        
        repo.commit('First Commit')
        self.wb = wb
        self.repo = repo
        
    def test_init(self):
        simple = self.repo.checkout('master')
        
        self.assertEqual(len(simple.ParentLinks),1)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, False)
        
        
    def test_modify_string(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.string, 'abc')
        simple.string = 'xyz'
        self.assertEqual(simple.string, 'xyz')
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    
    def test_modify_integer(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.integer, 5)

        simple.integer = 50
        self.assertEqual(simple.integer, 50)

        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_modify_float(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.float, 3.)
        
        simple.float = 50
        self.assertEqual(simple.float, 50.)
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_modify_strings(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.strings, ['stuff','junk','more'])
        
        simple.strings.insert(2,'xyz')
        self.assertEqual(simple.strings, ['stuff','junk','xyz','more'])
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    
    def test_modify_integers(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.integers, [1,2,3,4])
        simple.integers.append( 50)
        self.assertEqual(simple.integers, [1,2,3,4,50])
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_modify_floats(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        
        simple.floats.remove( 4.0)
        self.assertEqual(simple.floats, [3.,5.,6.])
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_setitem(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        simple.floats[1]=( 44.0)
        self.assertEqual(simple.floats, [3.,44.,5.,6.])
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_getitem(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        self.assertEqual(simple.floats[1], 4.)
        
        # Should not modify anything!
        self.assertEqual(len(simple.ParentLinks),1)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, False)
        
    def test_delitem(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        del simple.floats[1]
        self.assertEqual(simple.floats, [3.,5.,6.])
        
        
        # Should not modify anything!
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_delslice(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        del simple.floats[1:3]
        self.assertEqual(simple.floats, [3.,6.])
        
        
        # Should not modify anything!
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
        
    def test_invalidate(self):
        
        simple = self.repo.checkout('master')
        self.assertEqual(simple.floats, [3.,4.,5.,6.])
        floats = simple.floats
        
        # Checkout to invalidate it
        simple_2 = self.repo.checkout('master')
        
        self.assertEqual(object.__getattribute__(simple,'Invalid'), True)
        self.assertEqual(object.__getattribute__(floats,'Invalid'), True)
        

class StringDictObjectTest(unittest.TestCase):

    def setUp(self):
        self.wb = workbench.WorkBench('No Process Test')
        
        self.repo, self.sdobj = self.wb.init_repository(basic_pb2.StringDict)

    
    def test_add(self):
        """
        
        """
        self.sdobj.items.add()




class ContainerTest(unittest.TestCase):
        
    def setUp(self):
        self.wb = workbench.WorkBench('No Process Test')
        
        self.repo, self.listobj = self.wb.init_repository(basic_pb2.ListObj)
            
        
        
    def test_factory(self):
        
        items1 = self.listobj.items
        items2 = self.listobj.items
        
        self.assertIdentical(items1, items2)
        
        self.assertIdentical(items1._wrapper, self.listobj)
        self.assertIdentical(items1.Root, self.listobj.Root)

    def test_invalidation(self):
        """
        Test the container method to set link by index
        """
        kv1 = self.repo.create_wrapped_object(basic_pb2.KeyValue)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        
        items = self.listobj.items
        
        item1 = items.add()
        items.SetLink(0,kv1)
        
        self.repo.commit('test1')
        
        listobj = self.repo.checkout('master')
        
        # Test some internals of the container/checkout functionality
        self.assertEqual(items.Invalid, True)
        self.assertEqual(object.__getattribute__(self.listobj,'_invalid'), True)
        self.assertEqual(object.__getattribute__(kv1,'_invalid'), True)
        
    def test_add(self):
        """
        Test the container and set the link using the object wrapper
        """
        kv1 = self.repo.create_wrapped_object(basic_pb2.KeyValue)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        
        items = self.listobj.items
        
        item1 = items.add()
        item1.SetLink(kv1)
        
        self.assertEqual(kv1.IsRoot, True)
        self.assertNotIdentical(kv1.Root, items.Root)
        
        self.assertEqual(self.listobj.items[0].key, 'foo1')
        
        self.repo.commit('test1')
        
        listobj = self.repo.checkout('master')
        
        self.assertEqual(listobj.items[0].value, 'bar1')
        
        
    def test_setlink(self):
        """
        Test the container method to set link by index
        """
        kv1 = self.repo.create_wrapped_object(basic_pb2.KeyValue)
        kv1.key = 'foo1'
        kv1.value = 'bar1'
        
        items = self.listobj.items
        
        item1 = items.add()
        items.SetLink(0,kv1)
        
        # Test before commit to make sure the set/get works
        self.assertEqual(self.listobj.items[0].key, 'foo1')
        
        self.repo.commit('test1')
        
        listobj = self.repo.checkout('master')
        
        self.assertEqual(listobj.items[0].value, 'bar1')
        
    def test_getlink(self):
        """
        Must test before and after committing
        """
    def test_getlinks(self):
        """
        Must test before and after committing
        """
    def test_getitem(self):
        """
        Must test before and after committing
        """
    def test_getslice(self):
        """
        Must test before and after committing
        """
    def test_del(self):
        """
        Must test before and after committing
        """
    def test_delslice(self):
        """
        Must test before and after committing
        """
       
        
        
    #def testsetcontainerlink(self):
    #    kv1 = self.repo.create_wrapped_object(basic_pb2.KeyValue)
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
    #    listobj = self.repo.checkout('master')
    #    
    #    
        #
        #
        #
        #kv2 = repo.create_wrapped_object(basic_pb2.KeyValue)
        #kv2.key = 'foo2'
        #kv2.value = 'bar2'
        #
        #kv3 = repo.create_wrapped_object(basic_pb2.KeyValue)
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
        