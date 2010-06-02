#!/usr/bin/env python

"""
@fileion/play/rdf_store/blob_store.py
@author David Stuebe
@brief  RDF Base class tests
"""

import logging
from twisted.trial import unittest

from ion.play.rdf_store.rdf_base import RdfBase, RdfAssociation, RdfBlob, RdfEntity, RdfState, WorkSpace

class RdfTest(unittest.TestCase):
    """Testing service classes of resource registry
    """
    def setUp(self):
        self.key1='key1'
        self.key2='key2'
        
        self.val1='a'
        self.val2='b'
        self.val3='c'
    
    def test_RdfBase(self):
        
        base1 = RdfBase(self.val1,RdfBase.ASSOCIATION)
        base2 = RdfBase(self.val1,RdfBase.ENTITY,key=self.key1)
        self.assertNotEqual(base1,base2)
        base1 = RdfBase(self.val3,RdfBase.BLOB)
        base2 = RdfBase(self.val3,RdfBase.BLOB)
        self.assertEqual(base1,base2)
        print base1

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
        entity4.remove(l[4])
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
        state4.remove(l[4])
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

        alist=[assoc1,assoc2,assoc3,assoc4,assoc5]
        
        sortedkeys=RdfAssociation.sort_keys(alist)
        print ''
        for item in sortedkeys:
            
            print 'Kind: ',item,' count:', len(sortedkeys[item])

        self.assertEqual(2,len(sortedkeys[RdfBase.ASSOCIATION]))
        self.assertEqual(1,len(sortedkeys[RdfBase.STATE]))
        self.assertEqual(3,len(sortedkeys[RdfBase.BLOB]))
        self.assertEqual(1,len(sortedkeys[RdfBase.ENTITY]))


    def test_WorkSpace(self):
        
        blob1 = RdfBlob.create(self.val1)
        blob2 = RdfBlob.create(self.val2)
        blob3 = RdfBlob.create(self.val3)
        
        assoc1 = RdfAssociation.create(blob1,blob2,blob3)
        
        a_t = (assoc1, (blob1, blob2, blob3))
        
        w = WorkSpace.create([a_t])
        print ''
        w.print_status()
        print 'refs to blob1', w.len_refs(blob1)
        
        # How do you make this work?
        #self.assertFailure(w.make_rdf_reference(),RuntimeError())
        
        w.key=5
        ref = w.make_rdf_reference()
        
        comp = RdfEntity.create(assoc1,key=5)
        
        
        self.assertEqual(w.len_refs(blob1),1)
        
        self.assertEqual(w.len_associations(),1)
        self.assertEqual(w.len_blobs(),3)
        self.assertEqual(w.len_entities(),0)
        self.assertEqual(w.len_states(),0)


        w.remove_association(assoc1)
        w.print_status()
        print 'refs to blob1', w.len_refs(blob1)
        
        w.print_workspace()

        
        props={
            'name':'ctd',
            'model':'sbe911',
            'serial number':'932u8y74',
            'sensor ID':'293ulkskdj',
            'Manufacture':'SeaBird',
            'Point of Contact':'John Graybeal'
        }
        
        
        tuple1=('this',RdfBlob.create('OOI:Owner'),RdfEntity.reference('***Id of an Owner Entity/Statess'))
        tuple2=('this',RdfBlob.create('OOI:typeOf'),RdfEntity.reference('ID Instrument Resources'))
        
        associations=[tuple1,tuple2]
        
        res_description=WorkSpace.resource_properties('OOI:Instrument',props,associations)

        res_description.print_workspace()