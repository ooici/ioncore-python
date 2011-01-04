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


class WrapperMethodsTest(unittest.TestCase):
        
        
    def test_set_composite(self):
        
        wb = workbench.WorkBench('No Process Test')
        
        repo, ab = wb.init_repository(addressbook_pb2.AddressBook)
            
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
            
            repo, ab = wb.init_repository(addressbook_pb2.AddressLink)
            
            self.repo = repo
            self.ab = ab
            self.wb = wb
            
        def test_link(self):
            
            p = self.repo.create_wrapped_object(addressbook_pb2.Person)
                        
            p.name = 'David'
            self.ab.owner = p
            self.assertEqual(self.ab.owner.name ,'David')
            
        def test_composite_link(self):
            
            wL = self.ab.person.add()
            
            self.assertEqual(wL.GPBType, wL.LinkClassType)
            
            p = self.repo.create_wrapped_object(addressbook_pb2.Person)
            
            p.name = 'David'
            
            self.ab.person[0] = p
            
            self.assertEqual(self.ab.person[0].name ,'David')
            
        def test_dag(self):
            
            self.ab.person.add()
            
            p = self.repo.create_wrapped_object(addressbook_pb2.Person)
            
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
                
            ab2 = self.repo.create_wrapped_object(addressbook_pb2.AddressLink)
                
            self.ab.person[0] = ab2
                
            ab2.person.add()
            
            # Should fail due to circular reference!
            self.failUnlessRaises(repository.RepositoryError, ab2.person.SetLink, 0, self.ab)
            
            
        def test_clearfield(self):
            self.ab.person.add()
            p = self.repo.create_wrapped_object(addressbook_pb2.Person)
            p.name = 'David'
            p.id = 5
            self.ab.person[0] = p
            self.ab.owner = p

            self.assertEqual(self.ab.HasField('owner'),True)

            # Assert that there is a child link
            self.assertIn(self.ab.GetLink('owner'),self.ab.ChildLinks)
            self.assertIn(self.ab.person.GetLink(0),self.ab.ChildLinks)
            self.assertEqual(len(self.ab.ChildLinks),2)
            
            p.ParentLinks
            
            #Clear the field
            self.ab.ClearField('owner')
            # Assert that there is a child link
            self.assertIn(self.ab.person.GetLink(0),self.ab.ChildLinks)
            self.assertEqual(len(self.ab.ChildLinks),1)
            
            # The field is clear
            self.assertEqual(self.ab.HasField('owner'),False)
            
            
class RecurseCommitTest(unittest.TestCase):
        
            
    def test_simple_commit(self):
        wb = workbench.WorkBench('No Process Test')
            
        repo, ab = wb.init_repository(addressbook_pb2.AddressLink)
        
        ab.person.add()
        
        p = repo.create_wrapped_object(addressbook_pb2.Person)
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
        
            