#!/usr/bin/env python

"""
@file ion/play
@author David Stuebe
@test Service the protobuffers wrapper class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
#from twisted.internet import defer

from ion.test.iontest import IonTestCase

from net.ooici.play import addressbook_pb2

from ion.core.object import gpb_wrapper
from ion.core.object import repository
from ion.core.object import workbench
from ion.core.object import object_utils

person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)


class WrapperMethodsTest(unittest.TestCase):
        
        
    def test_set_composite(self):
        
        wb = workbench.WorkBench('No Process Test')
        
        repo, ab = wb.init_repository(addressbook_type)
            
        ab.person.add()
        ab.person.add()
            
        ab.person[1].name = 'David'
        ab.person[0].name = 'John'
        ab.person[0].id = 5
        
        p = ab.person[0]
        
        ph = ab.person[0].phone.add()
        ph.type = p.WORK
        ph.number = '123 456 7890'
            
        self.assertEqual(ab.person[1].name, 'David')
        self.assertEqual(ab.person[0].name, 'John')
        self.assertEqual(ab.person[0].id, 5)
            
        self.assertNotEqual(ab.person[0],ab.person[1])
        
        

class NodeLinkTest(unittest.TestCase):
            
            
        def setUp(self):
            wb = workbench.WorkBench('No Process Test')
            
            repo, ab = wb.init_repository(addresslink_type)
            
            self.repo = repo
            self.ab = ab
            self.wb = wb
            
        def test_link(self):
            
            p = self.repo.create_object(person_type)
                        
            p.name = 'David'
            self.ab.owner = p
            self.assertEqual(self.ab.owner.name ,'David')
            
        def test_composite_link(self):
            
            wL = self.ab.person.add()
            
            self.assertEqual(wL.ObjectType, wL.LinkClassType)
            
            p = self.repo.create_object(person_type)
            
            p.name = 'David'
            
            self.ab.person[0] = p
            
            self.assertEqual(self.ab.person[0].name ,'David')
            
        def test_dag(self):
            
            self.ab.person.add()
            
            p = self.repo.create_object(person_type)
            
            p.name = 'David'
            p.id = 5
            
            self.ab.person[0] = p
            
            self.ab.owner = p
            
            p.name ='John'
            
            self.assertEqual(self.ab.person[0].name ,'John')
            self.assertEqual(self.ab.owner.name ,'John')
                        
        #How do I make this a fail unless?   
        def test_inparents(self):
                
            self.ab.person.add()
                
            ab2 = self.repo.create_object(addresslink_type)
                
            self.ab.person[0] = ab2
                
            ab2.person.add()
            
            # Should fail due to circular reference!
            self.failUnlessRaises(repository.RepositoryError, ab2.person.SetLink, 0, self.ab)
            
            
        def test_clearfield(self):
            """
            Test clear field and the object accounting for parents and children
            """
            self.ab.person.add()
            p = self.repo.create_object(person_type)
            p.name = 'David'
            p.id = 5
            self.ab.person[0] = p
            self.ab.owner = p
            
            # Check to make sure all is set in the data structure
            self.assertEqual(self.ab.HasField('owner'),True)
            
            # Assert that there is a child link
            self.assertIn(self.ab.GetLink('owner'),self.ab.ChildLinks)
            self.assertIn(self.ab.person.GetLink(0),self.ab.ChildLinks)
            self.assertEqual(len(self.ab.ChildLinks),2)
            
            # Assert that there is a parent link
            self.assertIn(self.ab.GetLink('owner'),p.ParentLinks)
            self.assertIn(self.ab.person.GetLink(0),p.ParentLinks)
            self.assertEqual(len(p.ParentLinks),2)
            
            # Get the link object which will be cleared
            owner_link = self.ab.GetLink('owner')
            
            owner_link_hash = owner_link.GPBMessage.__hash__()
            owner_type_hash = owner_link.type.GPBMessage.__hash__()
            
            # Assert that the derived wrappers dictionary contains these objects
            self.assertIn(owner_link_hash, self.ab.DerivedWrappers.keys())
            self.assertIn(owner_type_hash, self.ab.DerivedWrappers.keys())
            
            # ***Clear the field***
            self.ab.ClearField('owner')
            
            # The field is clear
            self.assertEqual(self.ab.HasField('owner'),False)
            
            # Assert that there is only one child link
            self.assertIn(self.ab.person.GetLink(0),self.ab.ChildLinks)
            self.assertEqual(len(self.ab.ChildLinks),1)
            
            # Assert that there is only one parent link
            self.assertIn(self.ab.person.GetLink(0),p.ParentLinks)
            self.assertEqual(len(p.ParentLinks),1)
                        
            # Assert that the derived wrappers refs are gone!
            self.assertNotIn(owner_link_hash, self.ab.DerivedWrappers.keys())
            self.assertNotIn(owner_type_hash, self.ab.DerivedWrappers.keys())
            
            # Now try removing the person
            
            # Get the person link in the composite container
            p0_link = self.ab.person.GetLink(0)
            
            p0_link_hash = p0_link.GPBMessage.__hash__()
            p0_type_hash = p0_link.type.GPBMessage.__hash__()
            
            # Assert that the derived wrappers dictionary contains these objects
            self.assertIn(p0_link_hash, self.ab.DerivedWrappers.keys())
            self.assertIn(p0_type_hash, self.ab.DerivedWrappers.keys())
            
            # ***Clear the field***
            self.ab.ClearField('person')
            
            # The field is clear
            self.assertEqual(len(self.ab.person),0)
                
            # Assert that there are zero parent links
            self.assertEqual(len(p.ParentLinks),0)
            
            # Assert that there are zero child links
            self.assertEqual(len(self.ab.ChildLinks),0)
            
            # Assert that the derived wrappers dictionary is empty!
            self.assertNotIn(p0_link_hash, self.ab.DerivedWrappers.keys())
            self.assertNotIn(p0_type_hash, self.ab.DerivedWrappers.keys())
            self.assertEqual(len(self.ab.DerivedWrappers.keys()),1)
            
            
            
class RecurseCommitTest(unittest.TestCase):
        
            
    def test_simple_commit(self):
        wb = workbench.WorkBench('No Process Test')
            
        repo, ab = wb.init_repository(addresslink_type)
        
        ab.person.add()
        
        p = repo.create_object(person_type)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '123 456 7890'
        
        ab.person[0] = p
        
        ab.owner = p
            
        strct={}
            
        ab.RecurseCommit(strct)
        
        self.assertIn(ab.MyId, strct.keys())
        self.assertIn(p.MyId, strct.keys())
        
        # Get the committed structure element
        ab_se = strct.get(ab.MyId)
        
        self.assertEqual(len(ab_se.ChildLinks),1)
        self.assertIn(p.MyId, ab_se.ChildLinks)
        
            