#!/usr/bin/env python
"""
@Brief Test implementation of the repository class

@file ion/play
@author David Stuebe
@test Service the protobuffers wrapper class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


from twisted.trial import unittest

from ion.test.iontest import IonTestCase

from net.ooici.play import addressbook_pb2
from ion.core.object import workbench

from ion.core.object import repository

class RepositoryTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb
        
    def test_wrapper_properties(self):
        """
        Test the basic state of a new wrapper object when it is created
        """
        repo, simple = self.wb.init_repository(addressbook_pb2.AddressBook)   
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
        simple2= repo.create_wrapped_object(addressbook_pb2.AddressLink)
        
        self.assertEqual(len(simple2.ParentLinks),0)
        self.assertEqual(len(simple2.ChildLinks),0)
        self.assertEqual(simple2.IsRoot, True)
        self.assertEqual(simple2.Modified, True)
        
        
    def test_branch_checkout(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressBook)   
        p = ab.person.add()
        p.name = 'David'
        p.id = 1
        repo.commit()
        
        repo.branch(nickname="Arthur")
        
        self.assertEqual(p.name,'David')
        p.name = 'John'
        
        repo.commit()

        ab2 = repo.checkout(branchname="master")
        
        self.assertEqual(ab2.person[0].name, 'David')
        
        ab3 = repo.checkout(branchname="Arthur")
        
        self.assertEqual(ab3.person[0].name, 'John')
        
        
    def test_branch_no_commit(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)
        self.assertEqual(len(repo.branches),1)
            
        self.assertRaises(repository.RepositoryError, repo.branch, 'Arthur')
        
    def test_branch(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)
        repo.commit()
        self.assertEqual(len(repo.branches),1)

        repo.branch("Arthur")   
        self.assertEqual(len(repo.branches),2)
        
    def test_create_commit_ref(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)
        cref = repo._create_commit_ref(comment="Cogent Comment")
        assert(cref.comment == "Cogent Comment")
            
    def test_checkout_commit_id(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressBook)
        
        commit_ref1 = repo.commit()

        p = ab.person.add()
        p.id = 1
        p.name = 'Uma'
            
        commit_ref2 = repo.commit()
            
        p.name = 'alpha'
        commit_ref3 = repo.commit()
            
        ab = repo.checkout(branchname='master', commit_id=commit_ref1)
        self.assertEqual(len(ab.person),0)
            
        ab = repo.checkout(branchname='master', commit_id=commit_ref2)
        self.assertEqual(ab.person[0].id,1)
        self.assertEqual(ab.person[0].name,'Uma')
        
        ab = repo.checkout(branchname='master', commit_id=commit_ref3)
        self.assertEqual(ab.person[0].id,1)
        self.assertEqual(ab.person[0].name,'alpha')
        
        
    def test_log(self):
        wb1 = workbench.WorkBench('No Process Test')
        
        repo1, ab = self.wb.init_repository(addressbook_pb2.AddressBook)
        
        commit_ref1 = repo1.commit(comment='a')
        commit_ref2 = repo1.commit(comment='b')
        commit_ref3 = repo1.commit(comment='c')
            
            
        repo1.log_commits('master')
        
            
    def test_dag_structure(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)
                        
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '123 456 7890'
        
        ab.owner = p
            
        ab.person.add()
        ab.person[0] = p
        
        ab.person.add()
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='John'
        p.id = 78
        p.email = 'J@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '111 222 3333'
        
        ab.person[1] = p
        
        self.assertIdentical(ab.person[0], ab.owner)
        
        cref = repo.commit(comment='testing commit')
        
        p = None
        ab = None
        
        ab = repo.checkout(branchname='master')

        
        self.assertEqual(len(ab.ChildLinks),3)
        
        self.assertEqual(ab.person[0], ab.owner)
        self.assertIdentical(ab.person[0], ab.owner)
        
        ab.owner.name = 'Michael'
        
        
        self.assertEqual(ab.owner.name, 'Michael')
        
        self.assertEqual(ab.person[0].name, 'Michael')
 
 
    def test_lost_objects(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)
            
        # Create a resource object    
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '123 456 7890'
 
        ab.owner = p
        
        cref = repo.commit(comment='testing commit')
 
        ab2 = repo.checkout(branchname='master')
        
        # This is only for testing - to make sure that the invalid method is
        # working properly!
        self.assertEqual(object.__getattribute__(p,'Invalid'), True)
        self.assertEqual(object.__getattribute__(ab,'Invalid'), True)


 