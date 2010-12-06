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
        simple.float = 3.14159

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
        simple.string = 'xyz'
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    
    def test_modify_integer(self):
        
        simple = self.repo.checkout('master')
        simple.integer = 50
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_modify_float(self):
        
        simple = self.repo.checkout('master')
        simple.float = 50
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_modify_strings(self):
        
        simple = self.repo.checkout('master')
        simple.strings.insert(2,'xyz')
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    
    def test_modify_integers(self):
        
        simple = self.repo.checkout('master')
        simple.integers.append( 50)
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
    def test_modify_floats(self):
        
        simple = self.repo.checkout('master')
        simple.floats.remove( 4.0)
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
        
        
        
        
        
        