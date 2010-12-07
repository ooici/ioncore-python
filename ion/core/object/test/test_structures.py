#!/usr/bin/env python
"""
@Brief Test possible data structures

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
        
        
        
        
        