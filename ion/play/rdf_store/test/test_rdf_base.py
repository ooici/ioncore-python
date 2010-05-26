#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging
from twisted.trial import unittest

from ion.play.rdf_store.rdf_base import RdfBase, RdfAssociation, RdfBlob, RdfEntity, RdfState

class RdfTest(unittest.TestCase):
    """Testing service classes of resource registry
    """
    def setUp(self):
        self.key1='key1'
        self.key2='key2'
        
        self.val1='a'
        self.val2='b'
        self.val3='b'
    
    def test_RdfBase(self):
        
        base1 = RdfBase(self.val1,RdfBase.ASSOCIATION)
        base2 = RdfBase(self.val1,RdfBase.ENTITY,key=self.key1)
        self.assertNotEqual(base1,base2)
        base1 = RdfBase(self.val3,RdfBase.BLOB)
        base2 = RdfBase(self.val3,RdfBase.BLOB)
        self.assertEqual(base1,base2)

    def test_RdfBlob(self):
        
        blob1 = RdfBlob.create(self.val1)
        blob2 = RdfBlob.create(self.val2)
        self.assertNotEqual(blob1,blob2)
        blob3 = RdfBlob.create(self.val2)
        self.assertEqual(blob3,blob2)
        
        blob4=RdfBlob.load(blob3.key, blob3.object)
        self.assertEqual(blob3,blob4)

        
        
    def test_RdfEntity(self):
        
        blob2 = RdfBlob.create(self.val2)
        blob3 = RdfBlob.create(self.val3)
        l=list()
        for i in range(5):
            l.append(RdfAssociation.create(RdfBlob.create(i),blob2,blob3))
        
        entity1 = RdfEntity.create(l)
        
        entity2 = RdfEntity.create(l)
        
        # UUID Keys are different
        self.assertNotEqual(entity1,entity2)
        
        entity3 = RdfEntity.create(l,key=entity2.key)
        self.assertEqual(entity2,entity3)

        entity4 = RdfEntity.load(entity2.key,entity2.object)
        self.assertEqual(entity4,entity3)
        
        
        # Test discarding one
        entity4.discard(l[4])
        l.pop()
        entity5= RdfEntity.create(l,key=entity2.key)

        self.assertEqual(entity4,entity5)
        
        
    def test_RdfState(self):
        
        blob2 = RdfBlob.create(self.val2)
        blob3 = RdfBlob.create(self.val3)
        l=list()
        for i in range(5):
            l.append(RdfAssociation.create(RdfBlob.create(i),blob2,blob3))
        
        # 
        state1 = RdfState.create(self.key1,l,self.key2)

        l=list()
        for i in range(5):
            l.append(RdfAssociation.create(RdfBlob.create(-i),blob2,blob3))
        state2 = RdfState.create(self.key1,l,self.key2)
        
        # The Lists are different
        self.assertNotEqual(state1,state2)
        
        state3 = RdfState.create(state2.key,l,state2.commitRefs)
        self.assertEqual(state2,state3)

        state4 = RdfState.load(state2.key,state2.object,state2.commitRefs)
        self.assertEqual(state4,state3)
        
        # Test discarding one
        state4.discard(l[4])
        l.pop()
        state5= RdfState.create(self.key1,l,self.key2)

        self.assertEqual(state4,state5)

        

        
    def test_RdfAssociation(self):
        
        blob1 = RdfBlob.create(self.val1)
        blob2 = RdfBlob.create(self.val2)
        blob3 = RdfBlob.create(self.val3)
        
        l=list()
        for i in range(5):
            l.append(RdfAssociation.create(RdfBlob.create(i),blob2,blob3))
        
        entity1 = RdfEntity.create(l)

        state1 = RdfState.create(self.key1,l,['commitRef']) # States are initialized with a hash commit ref not a key!

        assoc1 = RdfAssociation.create(blob1,blob2,blob3)
        assoc2 = RdfAssociation.create(blob1,assoc1,entity1)
        assoc3 = RdfAssociation.create(entity1,blob2,state1)
        assoc4 = RdfAssociation.create(assoc2,entity1,state1)
        # None are equal...
        self.assertNotEqual(assoc1,assoc3)

        assoc5 = RdfAssociation.load(assoc3.key,assoc3.object)
        
        self.assertEqual(assoc3,assoc5)

