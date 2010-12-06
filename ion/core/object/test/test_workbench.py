#!/usr/bin/env python
"""
@Brief Test implementation of the workbench class

@file ion/core/object
@author David Stuebe
@test The object managment WorkBench class
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


class WorkBenchTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb
        
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
        
        self.ab = ab
        self.repo = repo
        
        
            
    def test_simple_commit(self):
        
        cref = self.repo.commit(comment='testing commit')
        print 'Commited',cref
        
        
    
    def test_pack_mutable(self):
        serialized = self.wb.pack_structure(self.repo._dotgit)
        
        
    def test_pack_root_eq_unpack(self):
        
        serialized = self.wb.pack_structure(self.ab)
            
        res = self.wb.unpack_structure(serialized)
        
        self.assertEqual(res,self.ab)
        
        
    def test_pack_mutable_eq_unpack(self):
            
        serialized = self.wb.pack_structure(self.repo._dotgit)
            
        repo = self.wb.unpack_structure(serialized)
        
        self.assertEqual(repo._dotgit, self.repo._dotgit)
        
        ab=repo.checkout(branchname='master')
        
        self.assertEqual(ab.person[0].name, 'David')
            
            
        
    def test_pack_repository_commits(self):
        
        self.repo.commit('testing repository packing')
        
        serialized = self.wb.pack_repository_commits(self.repo)
        
        repo = self.wb.unpack_structure(serialized)
        
        self.assertEqual(repo._dotgit, self.repo._dotgit)
        
        commit = repo.branches[0].commitrefs[0]
        
        #Check that the commit came through in the current branch
        self.assertEqual(commit, self.repo._current_branch.commitrefs[0])
        
        
    def test_init_repo(self):
            
        # Try it with no arguments
        repo, rootobj = self.wb.init_repository()
            
        rkey = repo.repository_key
        self.assertEqual(repo, self.wb.get_repository(rkey))
        self.assertEqual(rootobj, None)
            
            
        # Try it with a root object this time
        repo, rootobj = self.wb.init_repository(rootclass=addressbook_pb2.AddressBook)
            
        rkey = repo.repository_key
        self.assertEqual(repo, self.wb.get_repository(rkey))
        self.assertIsInstance(rootobj, gpb_wrapper.Wrapper)
            
        # Try it with a nickname for the repository
        repo, rootobj = self.wb.init_repository(rootclass=addressbook_pb2.AddressBook, nickname='David')
            
        self.assertEqual(repo, self.wb.get_repository('David'))
        self.assertIsInstance(rootobj, gpb_wrapper.Wrapper)
        
        
        
class WorkBenchMergeTest(unittest.TestCase):
        
    def test_fastforward_merge(self):
        wb1 = workbench.WorkBench('No Process Test')
        
        repo1, ab = wb1.init_repository(addressbook_pb2.AddressBook)
        
        commit_ref1 = repo1.commit(comment='a')
        commit_ref2 = repo1.commit(comment='b')
        commit_ref3 = repo1.commit(comment='c')
            
        repo1.log_commits('master')
            
        # Serialize it
        serialized = wb1.pack_repository_commits(repo1)
        
        # Create a new, separate work bench and read it!
        wb2 = workbench.WorkBench('No Process Test')
        repo2 = wb2.unpack_structure(serialized)
        
        repo2.log_commits('master')
        
        # Show that the state of the heads is the same
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        
        # Add more commits in repo 1
        commit_ref4 = repo1.commit(comment='d')
        commit_ref5 = repo1.commit(comment='e')
        commit_ref6 = repo1.commit(comment='f')
        
        # Serialize it
        serialized = wb1.pack_repository_commits(repo1)
        
        # Create a new, separate work bench and read it!
        repo2 = wb2.unpack_structure(serialized)
        
        repo2.log_commits('master')
        
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        
        
        
    def test_divergent_merge(self):
        wb1 = workbench.WorkBench('No Process Test')
        
        repo1, ab1 = wb1.init_repository(addressbook_pb2.AddressBook)
        
        commit_ref_a1 = repo1.commit(comment='a1')
        commit_ref_b1 = repo1.commit(comment='b1')
        commit_ref_c1 = repo1.commit(comment='c1')
            
        #repo1.log_commits('master')
            
        # Serialize it
        #serialized = wb1.pack_repository_commits(repo1)
        serialized = wb1.pack_structure(repo1._dotgit)
        
        
        # Create a new, separate work bench and read it!
        wb2 = workbench.WorkBench('No Process Test')
        repo2 = wb2.unpack_structure(serialized)
        
        print 'DOTGIT MYID', repo2._dotgit.MyId
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        #repo2.log_commits('master')
        
        ab2 = repo2.checkout('master')
        
        # Show that the state of the heads is the same
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        
        print 'DOTGIT MYID', repo2._dotgit.MyId
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        # add a commit on repo2!
        commit_ref_d2 = repo2.commit(comment='d2')
        
        print 'DOTGIT MYID', repo2._dotgit.MyId
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        # Add more commits in repo 1
        commit_ref_d1 = repo1.commit(comment='d1')
        commit_ref_e1 = repo1.commit(comment='e1')
        commit_ref_f1 = repo1.commit(comment='f1')
        
        # Serialize it
        serialized = wb1.pack_structure(repo1._dotgit)
        
        print 'DOTGIT MYID', repo2._dotgit.MyId
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        # Read it in the other work bench!
        repo2 = wb2.unpack_structure(serialized)
        
        repo2.log_commits('master')
        
        self.assertEqual(repo2.repository_key, repo1.repository_key)
        self.assertEqual(repo2.branches[0].branchkey, repo1.branches[0].branchkey)
        self.assertEqual(repo2.branches[0].commitrefs[1], repo1.branches[0].commitrefs[0])
        
        # Merge the coflict
        print 'DOTGIT MYID', repo2._dotgit.MyId
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        ab2 = repo2.checkout('master')
        
        # add a commit on repo2!
        commit_ref_d2 = repo2.commit(comment='g2')
        
        
        # Serialize it - to push back to repo1
        serialized = wb2.pack_structure(repo2._dotgit)
        
        # Read it in the other work bench!
        repo1 = wb1.unpack_structure(serialized)
        
        log.info('Showing merged history!')
        repo1.log_commits('master')
        
        # Show that the state of the heads is the same
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        
        
        
                        
        
        