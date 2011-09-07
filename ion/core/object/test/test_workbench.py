#!/usr/bin/env python
"""
@brief Test implementation of the workbench class

@file ion/core/object/test/test_workbench
@author David Stuebe
@test The object management WorkBench test class

@TODO Add better testing for excluded types and fetch_blobs/fetch_links
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer

from net.ooici.play import addressbook_pb2

from ion.core.object import gpb_wrapper
from ion.core.object import workbench
from ion.core.object import object_utils

# For testing the message based ops of the workbench
from ion.core.process.process import ProcessFactory, Process
from ion.test.iontest import IonTestCase



PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)
ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
ADDRESSBOOK_TYPE = object_utils.create_type_identifier(object_id=20002, version=1)
INVALID_TYPE = object_utils.create_type_identifier(object_id=-1, version=1)
PREDICATE_TYPE = object_utils.create_type_identifier(object_id=14, version=1)


class WorkBenchTest(unittest.TestCase):

    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb

        repo = self.wb.create_repository(ADDRESSLINK_TYPE)

        ab = repo.root_object

        p = repo.create_object(PERSON_TYPE)
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
        p = repo.create_object(PERSON_TYPE)
        p.name='John'
        p.id = 78
        p.email = 'J@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '111 222 3333'

        ab.person[1] = p

        self.ab = ab
        self.repo = repo

    def test_INVALID_TYPE(self):

        self.assertRaises(workbench.WorkBenchError, self.wb.init_repository, INVALID_TYPE )

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
        repo = self.wb.create_repository(ADDRESSBOOK_TYPE)
        rootobj = repo.root_object

        rkey = repo.repository_key
        self.assertEqual(repo, self.wb.get_repository(rkey))
        self.assertIsInstance(rootobj, gpb_wrapper.Wrapper)

        # Try it with a nickname for the repository
        repo = self.wb.create_repository(root_type=ADDRESSBOOK_TYPE, nickname='David')
        rootobj = repo.root_object

        self.assertEqual(repo, self.wb.get_repository('David'))
        self.assertIsInstance(rootobj, gpb_wrapper.Wrapper)

    def test_associations(self):

        # Copy the address book object from the setup method to three new objects and use them in an association
        self.ab.title = 'subject'
        subject = self.wb.create_repository(ADDRESSLINK_TYPE)
        subject.root_object = self.ab
        subject.commit('a subject')

        predicate = self.wb.create_repository(PREDICATE_TYPE)
        predicate.root_object.word = 'predicate'
        predicate.commit('a predicate')

        self.ab.title = 'object'
        obj = self.wb.create_repository(ADDRESSLINK_TYPE)
        obj.root_object = self.ab
        obj.commit('a object')


        association = self.wb.create_association(subject, predicate, obj)

        self.assertEqual(association.SubjectReference.key, subject.repository_key)
        self.assertEqual(association.PredicateReference.key, predicate.repository_key)
        self.assertEqual(association.ObjectReference.key, obj.repository_key)

        self.assertIn(association, subject.associations_as_subject.get_associations())
        self.assertIn(association, subject.associations_as_subject.get_associations_by_predicate(predicate.repository_key))

        self.assertIn(association, obj.associations_as_object.get_associations())
        self.assertIn(association, obj.associations_as_object.get_associations_by_predicate(predicate.repository_key))

        self.assertIn(association, predicate.associations_as_predicate.get_associations())
        self.assertIn(association, predicate.associations_as_predicate.get_associations_by_predicate(predicate.repository_key))


    def test_clear_non_persistent(self):
        """
        Call clear on a non persistent repository and make sure it is gone from the workbench
        """
        key = self.repo.repository_key

        self.assertEqual(self.wb.get_repository(key), self.repo)


        self.assertIn(key, self.wb._repos)

        log.info('Workbench state:\n' + str(self.wb))

        context = self.repo.convid_context
        self.wb.manage_workbench_cache(convid_context=context)

        # make sure it is gone!
        self.assertNotIn(key, self.wb._repos)
        self.assertEqual(self.wb.get_repository(key), None)


    def test_clear_persistent(self):
        """
        Call clear on a persistent repository and make sure it stays
        """

        key = self.repo.repository_key

        self.assertEqual(self.wb.get_repository(key), self.repo)

        self.repo.persistent = True

        context = self.repo.convid_context
        self.wb.manage_workbench_cache(convid_context=context)

        self.assertIn(key, self.wb._repos)

        self.assertEqual(self.wb.get_repository(key), self.repo)


    def test_cache_non_persistent(self):

        self.repo.commit('junk')

        key = self.repo.repository_key

        self.assertEqual(self.wb.get_repository(key), self.repo)

        self.repo.cached = True

        # Still there...
        self.assertIn(key, self.wb._repos)
        self.assertNotIn(key, self.wb._repo_cache)

        # Move it to the cache
        context = self.repo.convid_context
        self.wb.manage_workbench_cache(convid_context=context)

        # Make sure it is in the right place
        self.assertNotIn(key, self.wb._repos)
        self.assertIn(key, self.wb._repo_cache)

        # Get it back again
        self.assertEqual(self.wb.get_repository(key), self.repo)

        # back again...
        self.assertIn(key, self.wb._repos)
        self.assertNotIn(key, self.wb._repo_cache)


    def test_manage_cache_context(self):

        self.repo.commit('junk')

        self.repo.convid_context = 'mine!'

        key = self.repo.repository_key

        self.assertEqual(self.wb.get_repository(key), self.repo)

        self.repo.cached = True

        # Still there...
        self.assertIn(key, self.wb._repos)
        self.assertNotIn(key, self.wb._repo_cache)

        # Decided to change this behavior - no arguments should clear...
        """
        # Call manage without context
        self.wb.manage_workbench_cache()

        # Still there...
        self.assertIn(key, self.wb._repos)
        self.assertNotIn(key, self.wb._repo_cache)
        """

        # Call manage other context
        self.wb.manage_workbench_cache('Not Mine')

        # Still there...
        self.assertIn(key, self.wb._repos)
        self.assertNotIn(key, self.wb._repo_cache)

        # Call manage with context
        self.wb.manage_workbench_cache('mine!')


        # Make sure it is in the right place
        self.assertNotIn(key, self.wb._repos)
        self.assertIn(key, self.wb._repo_cache)

        # Get it back again
        self.assertEqual(self.wb.get_repository(key), self.repo)

        # back again...
        self.assertIn(key, self.wb._repos)
        self.assertNotIn(key, self.wb._repo_cache)



class WorkBenchProcess(Process):
    """
    A test process which has the ops of the workbench
    """


    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.

        Process.__init__(self, *args, **kwargs)



        self.op_pull = self.workbench.op_pull
        self.op_push = self.workbench.op_push
        self.op_fetch_blobs = self.workbench.op_fetch_blobs
        self.op_checkout = self.workbench.op_checkout



factory = ProcessFactory(WorkBenchProcess)



class WorkBenchProcessTest(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

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

        child_proc2 = yield sup.get_child_id('workbench_test2')
        log.info('Process ID:' + str(child_proc2))
        workbench_process2 = self._get_procinstance(child_proc2)


        repo = workbench_process1.workbench.create_repository(ADDRESSLINK_TYPE)

        ab = repo.root_object

        p = repo.create_object(PERSON_TYPE)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '123 456 7890'

        ab.owner = p

        ab.title = 'an addressbook'

        self.cref1 = repo.commit('Made it - few!')

        self.proc1 = workbench_process1
        self.proc2 = workbench_process2
        self.repo1 = repo



    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_pull(self):

        # Must make the repo persistent to compare the result
        self.repo1.persistent = True

        log.info('Pulling from: %s' % str(self.proc1.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        self.assertEqual(repo2._workspace_root, None)

        # Objects are sent in the pull (get_head_content is True by default)
        crefs = repo2.current_heads()
        self.assertEqual(len(crefs), 1)
        # The pull got the current head state...
        self.assertEqual(crefs[0].objectroot.title, 'an addressbook')

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

    @defer.inlineCallbacks
    def test_pull_invalid(self):

        log.info('Pulling from: %s' % str(self.proc1.id.full))
        yield self.failUnlessFailure(self.proc2.workbench.pull(self.proc1.id.full, 'foobar'), workbench.WorkBenchError)


    @defer.inlineCallbacks
    def test_pull_latest(self):

        # Must make the repo persistent to compare the result
        self.repo1.persistent = True


        # Get the current head object key - it will not be sent in the pull
        old_key = self.repo1.root_object.MyId

        # update and commit an new head object
        self.repo1.root_object.title = 'New Addressbook'
        self.repo1.commit('An updated addressbook')

        log.info('Pulling from: %s' % str(self.proc1.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        self.assertEqual(repo2._workspace_root, None)

        # Objects are sent in the pull (get_head_content is True)
        crefs = repo2.current_heads()
        self.assertEqual(len(crefs), 1)
        self.assertEqual(crefs[0].objectroot.title, 'New Addressbook')

        # The old stuff is not there!
        old_ref = crefs[0].parentrefs[0].commitref
        self.assertRaises(KeyError, getattr, old_ref, 'objectroot')

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

        self.assertNotIn(old_key, repo2.index_hash.keys())


    @defer.inlineCallbacks
    def test_pull_latest_checkout(self):

        # Must make the repo persistent to compare the result
        self.repo1.persistent = True

        # Get the current head object key - it will not be sent in the pull
        old_key = self.repo1.root_object.MyId

        # update and commit an new head object
        self.repo1.root_object.title = 'New Addressbook'
        self.repo1.commit('An updated addressbook')

        log.info('Pulling from: %s' % str(self.proc1.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key, get_head_content=False)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)


        self.assertEqual(repo2._workspace_root, None)
        # Objects are not sent in the pull (get_head_content is False)
        crefs = repo2.current_heads()
        self.assertEqual(len(crefs), 1)
        self.assertRaises(KeyError, getattr, crefs[0], 'objectroot')


        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

        self.assertNotIn(old_key, repo2.index_hash.keys())


        # Now check out the old version
        yield repo2.checkout(branchname = 'master',commit_id=self.cref1)
        yield self.repo1.checkout(branchname = 'master',commit_id=self.cref1)


        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

        self.assertIn(old_key, repo2.index_hash.keys())

    @defer.inlineCallbacks
    def test_pull_twice(self):

        # Must make the repo persistent to compare the result
        self.repo1.persistent = True

        log.info('Pulling from: %s' % str(self.proc1.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # Can't easily test that the messaging works properly - but make sure result is good
        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)



    @defer.inlineCallbacks
    def test_pull_update(self):

        # Must make the repo persistent to compare the result
        self.repo1.persistent = True

        log.info('Pulling from: %s' % str(self.proc1.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


        # update and commit an new head object
        self.repo1.root_object.title = 'New Addressbook'
        self.repo1.commit('An updated addressbook')


        # Pull the repository again and watch the merge magic!
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        ab = yield repo2.checkout('master')

        # Can't easily test that the messaging works properly - but make sure result is good
        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


    @defer.inlineCallbacks
    def test_pull_branch(self):


        # Must make the repo persistent to compare the result
        self.repo1.persistent = True

        self.branch_key = self.repo1.branch()

        self.repo1.root_object.title = 'branch'

        self.repo1.commit('Branched')

        log.info('Pulling from: %s' % str(self.proc1.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)


        # Objects are sent in the pull (get_head_content is True)
        crefs = repo2.current_heads()
        self.assertEqual(len(crefs), 2)
        # In this test we know the order but generally that is not true
        self.assertEqual(crefs[0].objectroot.title, 'an addressbook')
        self.assertEqual(crefs[1].objectroot.title, 'branch')


        ab = yield repo2.checkout(branchname=self.branch_key)

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

    @defer.inlineCallbacks
    def test_pull_diverge(self):


        log.info('Pushing to: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

        ab.title='Other Title'
        repo2.commit('Another updated addressbook')

        # update and commit an new head object
        ### Different Content
        #self.repo1.root_object.title = 'This Title'
        #self.repo1.commit('An updated addressbook')
        ### SAME CONTENT....
        self.repo1.root_object.title = 'Other Title'
        self.repo1.commit('Another updated addressbook')

        self.repo1.persistent=True

        # Push the object in proc2 back to proc1 - it will have both divergent states
        log.info('Pushing back to proc1')
        result = yield self.proc2.workbench.push(self.proc1.id.full, repo2)

        self.assertEqual(len(self.repo1.branches[0].commitrefs),2)

        log.info('Clearing proc2 workbench!')
        self.proc2.workbench.clear_repository(repo2)

        log.info('Pulling to: %s' % str(self.proc2.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key,get_head_content=False)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)


        ab = yield repo2.checkout('master')

        # Show that merge by date on checkout resolved the conflict
        self.assertEqual(self.repo1.root_object, repo2.root_object)


    @defer.inlineCallbacks
    def test_pull_no_automerge(self):


        log.info('Pushing to: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

        ab.title='Other Title'
        repo2.commit('Another updated addressbook')

        # update and commit an new head object
        ### Different Content
        self.repo1.root_object.title = 'This Title'
        self.repo1.commit('An updated addressbook')
        ### SAME CONTENT....
        #self.repo1.root_object.title = 'Other Title'
        #self.repo1.commit('Another updated addressbook')

        self.repo1.persistent = True

        # Push the object in proc2 back to proc1 - it will have both divergent states
        log.info('Pushing back to proc1')
        result = yield self.proc2.workbench.push(self.proc1.id.full, repo2)

        self.assertEqual(len(self.repo1.branches[0].commitrefs),2)

        log.info('Clearing proc2 workbench!')
        self.proc2.workbench.clear_repository(repo2)

        log.info('Pulling to: %s' % str(self.proc2.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key,get_head_content=False)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        self.assertEqual(repo2.upstream, self.proc1.id.full)


        ab = yield repo2.checkout('master', auto_merge=False)

        # Show that merge by date on checkout resolved the conflict
        self.assertEqual(ab.title, 'an addressbook')




    @defer.inlineCallbacks
    def test_pull_branch_same_head(self):
        """
        Test that we can have more than one branch point to the same commit!
        """

        # Must make the repo persistent to compare the result
        self.repo1.persistent = True

        self.first_branch_key = self.repo1.current_branch_key()
        self.second_branch_key = self.repo1.branch()

        log.info('Pulling from: %s' % str(self.proc1.id.full))
        result = yield self.proc2.workbench.pull(self.proc1.id.full, self.repo1.repository_key)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)


        # Objects are sent in the pull (get_head_content is True)
        ab = yield repo2.checkout(branchname=self.first_branch_key)

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


        ab = yield repo2.checkout(branchname=self.second_branch_key)

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


    @defer.inlineCallbacks
    def test_push(self):

        log.info('Pushing to: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


    @defer.inlineCallbacks
    def test_push_two(self):


        # update and commit an new head object
        self.repo1.root_object.title = 'New Addressbook'
        self.repo1.commit('An updated addressbook')

        log.info('Pushing tpo: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


        yield repo2.checkout(branchname = 'master',commit_id=self.cref1)
        yield self.repo1.checkout(branchname = 'master',commit_id=self.cref1)

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


    @defer.inlineCallbacks
    def test_push_update(self):


        log.info('Pushing tpo: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

        # update and commit an new head object
        self.repo1.root_object.title = 'New Addressbook'
        self.repo1.commit('An updated addressbook')

        log.info('Pushing tpo: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


    @defer.inlineCallbacks
    def test_push_diverge(self):


        log.info('Pushing to: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)

        ab = yield repo2.checkout('master')

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)

        ab.title='Other Title'
        repo2.commit('Another updated addressbook')

        # update and commit an new head object
        self.repo1.root_object.title = 'This Title'
        self.repo1.commit('An updated addressbook')

        log.info('Pushing tpo: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)


        ab = yield repo2.checkout('master')

        # Show that merge by date on checkout resolved the conflict
        self.assertEqual(self.repo1.root_object, repo2.root_object)


    @defer.inlineCallbacks
    def test_push_branch_same_head(self):
        """
        Test that we can have more than one branch point to the same commit!
        """
        self.first_branch_key = self.repo1.current_branch_key()
        self.second_branch_key = self.repo1.branch()

        log.info('Pushing tpo: %s' % str(self.proc2.id.full))
        result = yield self.proc1.workbench.push(self.proc2.id.full, self.repo1)
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        # use the value - the key of the first to get it from the workbench on the 2nd
        repo2 = self.proc2.workbench.get_repository(self.repo1.repository_key)


        # Objects are sent in the pull (get_head_content is True)
        ab = yield repo2.checkout(branchname=self.first_branch_key)

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)


        ab = yield repo2.checkout(branchname=self.second_branch_key)

        self.assertEqual(self.repo1.commit_head, repo2.commit_head)
        self.assertEqual(self.repo1.root_object, repo2.root_object)