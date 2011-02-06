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

attribute_type = object_utils.create_type_identifier(object_id=10017, version=1)

class WrapperMethodsTest(unittest.TestCase):
        
        
    def test_set_get_del(self):
        
        ab = gpb_wrapper.Wrapper._create_object(addressbook_type)
        
        # set a string field
        ab.title = 'String'
        
        # Can not set a string field to an integer
        self.assertRaises(TypeError, setattr, ab , 'title', 6)
        
        # Get a string field
        self.assertEqual(ab.title, 'String')
        
        # Del ?
        try:
            del ab.title
        except AttributeError, ae:
            return
        
        self.fail('Attribute Error not raised by invalid delete request')
            
        
        
    def test_set_composite(self):
        
        ab = gpb_wrapper.Wrapper._create_object(addressbook_type)
            
        ab.person.add()
        ab.person.add()
            
        ab.person[1].name = 'David'
        ab.person[0].name = 'John'
        ab.person[0].id = 5
        
        p = ab.person[0]
        
        ph = ab.person[0].phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '123 456 7890'
            
        self.assertEqual(ab.person[1].name, 'David')
        self.assertEqual(ab.person[0].name, 'John')
        self.assertEqual(ab.person[0].id, 5)
            
        self.assertNotEqual(ab.person[0],ab.person[1])
        
        
    def test_enum_access(self):
        
        p = gpb_wrapper.Wrapper._create_object(person_type)
        
        # Get an enum
        self.assertEqual(p.PhoneType.MOBILE,0)
        self.assertEqual(p.PhoneType.HOME,1)
        self.assertEqual(p.PhoneType.WORK,2)
        
        # error to set an enum
        self.assertRaises(AttributeError, setattr, p.PhoneType, 'MOBILE', 5)
        
    def test_imported_enum_access(self):
        
        
        att = gpb_wrapper.Wrapper._create_object(attribute_type)
        
        att.data_type = att.DataType.BOOLEAN
        
        self.assertEqual(att.data_type, att.DataType.BOOLEAN)
        
        self.assertRaises(AttributeError, setattr, att.DataType, 'BOOLEAN', 5)
        
        
        
    def test_listsetfields(self):
        
        p = gpb_wrapper.Wrapper._create_object(person_type)

        p.name = 'David'
        p.id = 5
        
        # get a list of the names which are set
        mylist = p.ListSetFields()
        # returned list is in order of field identifier
        self.assertEqual(['name', 'id'], mylist)
        
        
        
class TestWrapperMethodsRequiringRepository(unittest.TestCase):
    
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        
        repo, ab = wb.init_repository(addresslink_type)
        
        self.repo = repo
        self.ab = ab
        self.wb = wb
        
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
        self.assertEqual(self.ab.IsFieldSet('owner'),True)
        
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
        self.assertEqual(self.ab.IsFieldSet('owner'),False)
        
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
        ph.type = p.PhoneType.WORK
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
        
    def test_dag_conflict(self):
        
        wb = workbench.WorkBench('No Process Test')
            
        repo = wb.create_repository(addresslink_type)
        
        repo.root_object.person.add()
        repo.root_object.person[0] = repo.create_object(person_type)
        repo.root_object.person[0].name = 'David'
        repo.root_object.person[0].id = 5
        
        repo.root_object.person.add()
        repo.root_object.person[1] = repo.create_object(person_type)
        repo.root_object.person[1].name = 'David'
        repo.root_object.person[1].id = 5
        
        p0 = repo.root_object.person[0]
        p1 = repo.root_object.person[1]
        
        strct={}
        repo.root_object.RecurseCommit(strct)
        
        
        # The address link should now be unmodified
        self.assertEqual(repo.root_object.Modified, False)
        
        # there should be only two objects once hashed!
        self.assertEqual(len(strct.keys()), 2)
        
        # Show that the old references are now invalid
        self.assertEqual(p0.Invalid, True)
        self.assertEqual(p1.Invalid, True)
        
        # manually update the hashed elements...
        wb._hashed_elements.update(strct)
        
        self.assertIn(repo.root_object.MyId, strct)
        self.assertIn(repo.root_object.person[0].MyId, strct)
        self.assertIn(repo.root_object.person[1].MyId, strct)
            
        self.assertIdentical(repo.root_object.person[0], repo.root_object.person[1])
            
        # Get the committed structure element
        ab_se = strct.get(repo.root_object.MyId)
            
        # Show that the the SE recongnized only 1 child object
        self.assertEqual(len(ab_se.ChildLinks),1)
            
        # There should be two parent links
        self.assertEqual(len(repo.root_object.person[0].ParentLinks), 2)
        
        par1 = repo.root_object.person[0].ParentLinks.pop()
        par2 = repo.root_object.person[0].ParentLinks.pop()
        
        # They are not identical - they came from a set, but they should be equal!
        self.assertEqual(par1, par2)
        
        # Their root should be identical, the addresslink!
        self.assertIdentical(par1.Root, repo.root_object)
        self.assertIdentical(par1.Root, par2.Root)
        
        
    def test_reset_same_value(self):
        
        wb = workbench.WorkBench('No Process Test')
            
        repo = wb.create_repository(addresslink_type)
        
        repo.root_object.person.add()
        repo.root_object.person[0] = repo.create_object(person_type)
        repo.root_object.person[0].name = 'David'
        repo.root_object.person[0].id = 5
        
        repo.commit('Jokes on me')
        
        repo.root_object.person[0].name = 'David'
        
        self.assertEqual(repo.root_object.Modified, True)
        
        p0 = repo.root_object.person[0]
        
        repo.commit('Jokes on you!')

        self.assertEqual(repo.root_object.Modified, False)
        self.assertEqual(p0.Invalid, False)
        self.assertEqual(p0.Modified, False)
        
        
        
            