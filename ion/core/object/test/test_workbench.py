#!/usr/bin/env python
"""
@brief Test implementation of the workbench class

@file ion/core/object/test/test_workbench
@author David Stuebe
@test The object management WorkBench test class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer

import weakref
import gc


from net.ooici.play import addressbook_pb2

from ion.core.object import gpb_wrapper
from ion.core.object import repository
from ion.core.object import workbench
from ion.core.object import object_utils

# For testing the message based ops of the workbench
from ion.core.process.process import ProcessFactory, Process
from ion.test.iontest import IonTestCase



person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)
invalid_type = object_utils.create_type_identifier(object_id=-1, version=1)


class WorkBenchTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb
        
        repo = self.wb.create_repository(addresslink_type)

        ab = repo.root_object
                        
        p = repo.create_object(person_type)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
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
        ph.type = p.PhoneType.WORK
        ph.number = '111 222 3333'
        
        ab.person[1] = p
        
        self.ab = ab
        self.repo = repo
        
    def test_invalid_type(self):
        
        self.assertRaises(workbench.WorkBenchError, self.wb.init_repository, invalid_type )
            
    def test_simple_commit(self):
        
        cref = self.repo.commit(comment='testing commit')
        self.assertEqual(cref, self.repo._current_branch.commitrefs[0].MyId)
        
        self.assertIn(self.ab.MyId, self.repo.index_hash.keys())
        self.assertIn(self.ab.person[0].MyId, self.repo.index_hash.keys())
        self.assertIn(self.ab.person[1].MyId, self.repo.index_hash.keys())
        self.assertIn(self.ab.owner.MyId, self.repo.index_hash.keys())
        
        self.assertIn(cref, self.repo.index_hash.keys())
        
        cref_se = self.repo.index_hash.get(cref)
        self.assertEqual(len(cref_se.ChildLinks),1)
        self.assertIn(self.ab.MyId, cref_se.ChildLinks)

        
    def test_create_repo(self):
            
        # Try it with no arguments
        repo = self.wb.create_repository()   
        rootobj = repo.root_object
            
        rkey = repo.repository_key
        self.assertEqual(repo, self.wb.get_repository(rkey))
        self.assertEqual(rootobj, None)
            
            
        # Try it with a root object this time
        repo = self.wb.create_repository(addressbook_type)
        rootobj = repo.root_object
            
        rkey = repo.repository_key
        self.assertEqual(repo, self.wb.get_repository(rkey))
        self.assertIsInstance(rootobj, gpb_wrapper.Wrapper)
            
        # Try it with a nickname for the repository
        repo = self.wb.create_repository(root_type=addressbook_type, nickname='David')
        rootobj = repo.root_object
            
        self.assertEqual(repo, self.wb.get_repository('David'))
        self.assertIsInstance(rootobj, gpb_wrapper.Wrapper)
        
    def test_associations(self):

        # Copy the address book object from the setup method to three new objects and use them in an association
        self.ab.title = 'subject'
        subject = self.wb.create_repository(addresslink_type)
        subject.root_object = self.ab
        subject.commit('a subject')

        self.ab.title = 'predicate'
        predicate = self.wb.create_repository(addresslink_type)
        predicate.root_object = self.ab
        predicate.commit('a predicate')

        self.ab.title = 'object'
        obj = self.wb.create_repository(addresslink_type)
        obj.root_object = self.ab
        obj.commit('a object')


        association = self.wb.create_association(subject, predicate, obj)

        self.assertEqual(association.root_object.subject.key, subject.repository_key)
        self.assertEqual(association.root_object.predicate.key, predicate.repository_key)
        self.assertEqual(association.root_object.object.key, obj.repository_key)


    def test_clear_non_persistent(self):
        """
        Call clear on a non persistent repository and make sure it is gone from the workbench
        """
        key = self.repo.repository_key

        self.assertEqual(self.wb.get_repository(key), self.repo)


        self.wb.clear_non_persistent()

        # make sure it is gone!
        self.assertEqual(self.wb.get_repository(key), None)


    def test_clear_persistent(self):
        """
        Call clear on a persistent repository and make sure it stays
        """

        key = self.repo.repository_key

        self.assertEqual(self.wb.get_repository(key), self.repo)

        self.repo.persistent = True

        self.wb.clear_non_persistent()


        self.assertEqual(self.wb.get_repository(key), self.repo)


class WorkBenchProcess(Process):
    """
    A test process which has the ops of the workbench
    """


    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.

        Process.__init__(self, *args, **kwargs)



        self.op_pull = self.workbench.op_pull



factory = ProcessFactory(WorkBenchProcess)



class WorkBenchProcessTest(IonTestCase):



    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_pull(self):

        processes = [
            {'name':'workbench_test1',
             'module':'ion.core.object.test.test_workbench',
             'class':'WorkBenchProcess',
             'spawnargs':{'proc-name':'wb1'}},

            {'name':'workbench_test2',
             'module':'ion.core.object.test.test_workbench',
             'class':'WorkBenchProcess',
             'spawnargs':{'proc-name':'wb2'}},
        ]

        sup = yield self._spawn_processes(processes)

        child_proc1 = yield sup.get_child_id('workbench_test1')
        log.info('Process ID:' + str(child_proc1))
        workbench_process1 = self._get_procinstance(child_proc1)


        repo = workbench_process1.workbench.create_repository(addresslink_type)

        ab = repo.root_object

        p = repo.create_object(person_type)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '123 456 7890'

        ab.owner = p

        repo.commit('Made it - few!')


        child_proc2 = yield sup.get_child_id('workbench_test2')
        log.info('Process ID:' + str(child_proc2))
        workbench_process2 = self._get_procinstance(child_proc2)


        log.info('Sending to: %s' % str(workbench_process1.id.full))
        yield workbench_process2.workbench.pull(workbench_process1.id.full, repo.repository_key)

        myrepo = workbench_process2.workbench.get_repository(repo.repository_key)
        print myrepo

        
class WorkBenchMergeTest(unittest.TestCase):
        
    '''
    REfactoring push and pull!
    def test_fastforward_merge(self):
        wb1 = workbench.WorkBench('No Process Test')
        
        repo1, ab = wb1.init_repository(addressbook_type)
        
        commit_ref1 = repo1.commit(comment='a')
        commit_ref2 = repo1.commit(comment='b')
        commit_ref3 = repo1.commit(comment='c')
            
        repo1.log_commits('master')
            
        # Serialize it
        serialized = wb1.pack_repository(repo1)
        
        # Create a new, separate work bench and read it!
        wb2 = workbench.WorkBench('No Process Test')
        heads = wb2.unpack_structure(serialized)
        
        repo2 = wb2._load_repo_from_mutable(heads[0])
        
        repo2.log_commits('master')
        
        # Show that the state of the heads is the same
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        
        # Add more commits in repo 1
        commit_ref4 = repo1.commit(comment='d')
        commit_ref5 = repo1.commit(comment='e')
        commit_ref6 = repo1.commit(comment='f')
        
        # Serialize it
        serialized = wb1.pack_repository(repo1)
        
        # Create a new, separate work bench and read it!
        heads = wb2.unpack_structure(serialized)
        
        repo2 = wb2._load_repo_from_mutable(heads[0])
        
        repo2.log_commits('master')
        
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        
        
    @defer.inlineCallbacks
    def test_divergent_merge(self):
        wb1 = workbench.WorkBench('No Process Test')
        
        repo1, ab1 = wb1.init_repository(addressbook_type)
        
        commit_ref_a1 = repo1.commit(comment='a1')
        commit_ref_b1 = repo1.commit(comment='b1')
        commit_ref_c1 = repo1.commit(comment='c1')
            
        #repo1.log_commits('master')
            
        # Serialize it
        #serialized = wb1.pack_repository_commits(repo1)
        serialized = wb1.pack_structure(repo1._dotgit)
        
        
        # Create a new, separate work bench and read it!
        wb2 = workbench.WorkBench('No Process Test')
        heads = wb2.unpack_structure(serialized)
        
        repo2 = wb2._load_repo_from_mutable(heads[0])
        
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        #repo2.log_commits('master')
        
        ab2 = yield repo2.checkout('master')
        
        # Show that the state of the heads is the same
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        # add a commit on repo2!
        commit_ref_d2 = repo2.commit(comment='d2')
        
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        # Add more commits in repo 1
        commit_ref_d1 = repo1.commit(comment='d1')
        commit_ref_e1 = repo1.commit(comment='e1')
        commit_ref_f1 = repo1.commit(comment='f1')
        
        # Serialize it
        serialized = wb1.pack_structure(repo1._dotgit)
        
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        # Read it in the other work bench!
        heads = wb2.unpack_structure(serialized)
        
        repo2 = wb2._load_repo_from_mutable(heads[0])
        
        repo2.log_commits('master')
        
        self.assertEqual(repo2.repository_key, repo1.repository_key)
        self.assertEqual(repo2.branches[0].branchkey, repo1.branches[0].branchkey)
        self.assertEqual(repo2.branches[0].commitrefs[1], repo1.branches[0].commitrefs[0])
        
        # Merge the coflict
        self.assertNotIn(repo2._dotgit.MyId, repo2._workspace)
        
        ab2 = repo2.checkout('master')
        
        # add a commit on repo2!
        commit_ref_d2 = repo2.commit(comment='g2')
        
        
        # Serialize it - to push back to repo1
        serialized = wb2.pack_structure(repo2._dotgit)
        
        # Read it in the other work bench!
        heads = wb1.unpack_structure(serialized)        
        repo1 = wb1._load_repo_from_mutable(heads[0])
        
        
        log.info('Showing merged history!')
        repo1.log_commits('master')
        
        # Show that the state of the heads is the same
        self.assertEqual(repo2._dotgit, repo1._dotgit)
        '''
        
        
                        
        
        