#!/usr/bin/env python
"""
@brief Test implementation of the repository class

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
from ion.core.object import gpb_wrapper
from ion.core.object import object_utils
from ion.core.object import repository

invalid_type = object_utils.create_type_identifier(object_id=-1, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)


class RepositoryTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb
        
        
    def test_init_repo(self):
        
        
        # Pass in a type id object (the new way!)
        repo2, person = self.wb.init_repository(person_type)
        self.assertIsInstance(repo2, repository.Repository)
        self.assertIsInstance(person, gpb_wrapper.Wrapper)
        
        # Pass in an invalid type id object (the new way!)
        self.assertRaises(workbench.WorkBenchError, self.wb.init_repository, invalid_type)
        
        # Pass in None - get a repo with no root object yet...
        repo3, nothing = self.wb.init_repository()
        self.assertIsInstance(repo3, repository.Repository)
        self.assertEqual(nothing, None)
        
        # Pass in an invalid argument
        self.assertRaises(workbench.WorkBenchError, self.wb.init_repository, 52)
        
        
    def test_wrapper_properties(self):
        """
        Test the basic state of a new wrapper object when it is created
        """
        repo, simple = self.wb.init_repository(addressbook_type)   
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
        simple2= repo.create_object(addresslink_type)
        
        self.assertEqual(len(simple2.ParentLinks),0)
        self.assertEqual(len(simple2.ChildLinks),0)
        self.assertEqual(simple2.IsRoot, True)
        self.assertEqual(simple2.Modified, True)
        
        
    def test_branch_checkout(self):
        repo, ab = self.wb.init_repository(addressbook_type)   
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
        repo, ab = self.wb.init_repository(addresslink_type)
        self.assertEqual(len(repo.branches),1)
            
        self.assertRaises(repository.RepositoryError, repo.branch, 'Arthur')
        
    def test_branch(self):
        repo, ab = self.wb.init_repository(addresslink_type)
        repo.commit()
        self.assertEqual(len(repo.branches),1)

        repo.branch("Arthur")   
        self.assertEqual(len(repo.branches),2)
        
    def test_create_commit_ref(self):
        repo, ab = self.wb.init_repository(addresslink_type)
        cref = repo._create_commit_ref(comment="Cogent Comment")
        assert(cref.comment == "Cogent Comment")
            
    def test_checkout_commit_id(self):
        repo, ab = self.wb.init_repository(addressbook_type)
        
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
        
        repo1, ab = self.wb.init_repository(addressbook_type)
        
        commit_ref1 = repo1.commit(comment='a')
        commit_ref2 = repo1.commit(comment='b')
        commit_ref3 = repo1.commit(comment='c')
            
            
        repo1.log_commits('master')
        
            
    def test_dag_structure(self):
        repo, ab = self.wb.init_repository(addresslink_type)
                        
        p = repo.create_object(person_type)
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
        p = repo.create_object(person_type)
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
        repo, ab = self.wb.init_repository(addresslink_type)
            
        # Create a resource object    
        p = repo.create_object(person_type)
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


    def test_transfer_repository_objects(self):
        
        repo1, ab1 = self.wb.init_repository(addresslink_type)
            
        # Create a resource object    
        p1 = repo1.create_object(person_type)
        p1.name='David'
        p1.id = 5
        p1.email = 'd@s.com'
        ph1 = p1.phone.add()
        ph1.type = p1.WORK
        ph1.number = '123 456 7890'
 
        ab1.owner = p1
        
        cref = repo1.commit(comment='testing commit')
 
        # Create a second repository and copy from 1 to 2
        repo2, ab2 = self.wb.init_repository(addresslink_type)
            
        ab2.person.add()
        ab2.person[0] = p1
            
        self.assertEqual(ab2.person[0].name, 'David')
            
        self.assertEqual(ab2.person[0].MyId, ab1.owner.MyId)
        self.assertEqual(ab2.person[0], ab1.owner)
        self.assertNotIdentical(ab2.person[0], ab1.owner)
        self.assertNotIdentical(ab2.person[0].Repository, ab1.owner.Repository)
        
        self.assertIdentical(ab2.person[0].Repository, ab2.Repository)
        
        
 
 
 
 
 
 