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


class RepositoryTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb
        
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

        ab = repo.checkout(branchname="master")
        
        self.assertEqual(ab.person[0].name, 'David')
        
        ab = repo.checkout(branchname="Arthur")
        
        self.assertEqual(ab.person[0].name, 'John')
        
        
        
    def test_branch_no_commit(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)
        try:
            repo.branch("Arthur")
        except Exception, ex:
            log.debug(str(ex))
            
                
    def test_branch(self):
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)
        repo.commit()
        repo.branch("Arthur")   
        
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
        
            
    def test_simple_commit(self):
        
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
        
        cref = repo.commit(comment='testing commit')
        print cref
        
        print 'dotgit', repo._dotgit
        
        p = None
        ab = None
        
        ab = repo.checkout(branchname='master')
        print 'ab after checkout',ab
        
        print ab.person[0]
        print ab.person[1]
        
        
        for person in ab.person:
            print 'person',person
        print 'owner',ab.owner
        
        