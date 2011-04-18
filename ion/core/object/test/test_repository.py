#!/usr/bin/env python
"""
@brief Test implementation of the repository class

@file ion/play
@author David Stuebe
@test Service the protobuffers wrapper class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer


from twisted.trial import unittest

import weakref
import gc

from net.ooici.play import addressbook_pb2
from ion.core.object import workbench
from ion.core.object import gpb_wrapper
from ion.core.object import object_utils
from ion.core.object import repository

INVALID_TYPE = object_utils.create_type_identifier(object_id=-1, version=1)
PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)
ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
ADDRESSBOOK_TYPE = object_utils.create_type_identifier(object_id=20002, version=1)


class DummyClass(object):

        def __init__(self, *args, **kwargs):

            self.args = args
            self.kwargs = kwargs

class IndexHashTest(unittest.TestCase):

    def setUp(self):

        #self.cache = dict()
        self.cache = weakref.WeakValueDictionary()



    def test_bad_cache(self):

        ih = repository.IndexHash()
        # Must be a weak value Dictionary
        self.assertRaises(AssertionError, ih._set_cache, dict())


    def test_setitem_getitem(self):


        ih = repository.IndexHash()
        ih.cache = self.cache


        ih['a']=DummyClass(5,3,david='abc')


        # Get from the indexes objects
        self.assertIsInstance(dict.__getitem__(ih,'a'), DummyClass)
        self.assertRaises(KeyError, dict.__getitem__, ih,'v' )

        # Get from the cache
        self.assertIsInstance(self.cache['a'], DummyClass)
        self.assertRaises(KeyError, self.cache.__getitem__, 'v')


        # Get from the index hash
        self.assertIsInstance(ih['a'], DummyClass)
        self.assertRaises(KeyError, ih.__getitem__, 'v')


    def test_putitem_1_getitem_2(self):


        ih1 = repository.IndexHash()
        ih1.cache = self.cache


        ih1['a']=DummyClass(5,3,david='abc')


        # Get from the indexes objects
        self.assertIsInstance(dict.__getitem__(ih1,'a'), DummyClass)

        # Get from the cache
        self.assertIsInstance(self.cache['a'], DummyClass)

        # Get from the index hash
        self.assertIsInstance(ih1['a'], DummyClass)


        # Create a second IndexHash and test the objects...
        ih2 = repository.IndexHash()
        ih2.cache = self.cache

        # Get from the indexes objects - it does not have it yet...
        self.assertRaises(KeyError, dict.__getitem__, ih2,'a' )

        # Get from the cache
        self.assertEqual(ih2['a'].args, (5,3))

        # Now it has it!
        self.assertIsInstance(dict.__getitem__(ih2,'a'), DummyClass)


    def test_update_1_get_2(self):


        ih1 = repository.IndexHash()
        ih1.cache = self.cache

        ih1.update({'a':DummyClass(5,3,david='abc'),'b':DummyClass(55,33,david='def')})


        # Get from the indexes objects - test this first
        self.assertEqual(dict.__getitem__(ih1,'a').args, (5,3))
        self.assertEqual(dict.__getitem__(ih1,'b').args, (55,33))
        self.assertEqual(dict.__len__(ih1),2)


        # Get from the cache - make sure it got added
        self.assertIsInstance(self.cache['a'], DummyClass)
        self.assertIsInstance(self.cache['b'], DummyClass)
        self.assertEqual(len(self.cache),2)

        # Get from the index hash
        self.assertIsInstance(ih1.get('a'), DummyClass)
        self.assertEqual(ih1.get('b').args, (55,33))
        self.assertEqual(ih1.get('c'), None)

        # Make sure nothing got added...
        self.assertEqual(len(self.cache),2)
        self.assertEqual(len(ih1),2)
        self.assertEqual(dict.__len__(ih1),2)


        # Create a second IndexHash and test the objects...
        ih2 = repository.IndexHash()
        ih2.cache = self.cache

        # Get from the indexes objects - it does not have it yet...
        self.assertEqual(dict.get(ih2,'a'), None )
        self.assertEqual(dict.get(ih2,'b'), None )
        self.assertEqual(dict.get(ih2,'c'), None )

        # Get from the cache
        self.assertIsInstance(ih2.get('a'), DummyClass)
        self.assertIsInstance(ih2.get('b'), DummyClass)
        self.assertEqual(dict.get(ih2,'c'), None ) # Still None

        # Now it has it!
        self.assertEqual(dict.get(ih2,'a').args, (5,3) )
        self.assertEqual(dict.get(ih2,'b').args, (55,33) )
        self.assertEqual(dict.get(ih2,'c'), None ) # Still None

        # Make sure nothing got added...
        self.assertEqual(len(self.cache),2)
        self.assertEqual(len(ih2),2)
        self.assertEqual(dict.__len__(ih2),2)


    def test_add_cache_later(self):

        ih1 = repository.IndexHash()

        ih1.update({'a':DummyClass(5,3,david='abc'),'b':DummyClass(55,33,david='def')})

        # Get from the indexes objects - test this first
        self.assertEqual(dict.__getitem__(ih1,'a').args, (5,3))
        self.assertEqual(dict.__getitem__(ih1,'b').args, (55,33))
        self.assertEqual(dict.__len__(ih1),2)


        # Get from the cache - make sure it got added
        self.assertEqual(self.cache.get('a'), None)
        self.assertEqual(self.cache.get('b'), None)
        self.assertEqual(len(self.cache),0)

        # Get from the index hash
        self.assertIsInstance(ih1.get('a'), DummyClass)
        self.assertEqual(ih1.get('b').args, (55,33))
        self.assertEqual(ih1.get('c'), None)

        # Make sure nothing got added...
        self.assertEqual(len(self.cache),0)
        self.assertEqual(len(ih1),2)
        self.assertEqual(dict.__len__(ih1),2)

        # Now add the cache
        ih1.cache = self.cache

        # Get from the indexes objects - test this first
        self.assertEqual(dict.__getitem__(ih1,'a').args, (5,3))
        self.assertEqual(dict.__getitem__(ih1,'b').args, (55,33))
        self.assertEqual(dict.__len__(ih1),2)


        # Get from the cache - make sure it got added
        self.assertIsInstance(self.cache['a'], DummyClass)
        self.assertIsInstance(self.cache['b'], DummyClass)
        self.assertEqual(len(self.cache),2)

        # Get from the index hash
        self.assertIsInstance(ih1.get('a'), DummyClass)
        self.assertEqual(ih1.get('b').args, (55,33))
        self.assertEqual(ih1.get('c'), None)

        # Make sure nothing got added...
        self.assertEqual(len(self.cache),2)
        self.assertEqual(len(ih1),2)
        self.assertEqual(dict.__len__(ih1),2)


    def test_weakvalues(self):

        ih1 = repository.IndexHash()
        ih1.cache = self.cache

        ih1.update({'a':DummyClass(5,3,david='abc'),'b':DummyClass(55,33,david='def')})


        self.assertEqual(len(gc.get_referrers(self.cache['a'])),1)

        del ih1['a']

        self.assertEqual(self.cache.has_key('a'),False)


        ih2 = repository.IndexHash()
        ih2.cache = self.cache

        # Bring the k,v into ih2
        ih2.get('b')
        self.assertEqual(len(gc.get_referrers(self.cache['b'])),2)

        # Delete it from ih1
        del ih1['b']

        # Still in the cache!
        self.assertEqual(self.cache.has_key('b'),True)
        self.assertEqual(len(gc.get_referrers(self.cache['b'])),1)





class RepositoryTest(unittest.TestCase):

    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb
        
    def test_init_repo(self):
        
        
        # Pass in a type id object (the new way!)
        repo2, person = self.wb.init_repository(PERSON_TYPE)
        self.assertIsInstance(repo2, repository.Repository)
        self.assertIsInstance(person, gpb_wrapper.Wrapper)
        
        # Pass in an invalid type id object (the new way!)
        self.assertRaises(workbench.WorkBenchError, self.wb.init_repository, INVALID_TYPE)
        
        # Pass in None - get a repo with no root object yet...
        repo3, nothing = self.wb.init_repository()
        self.assertIsInstance(repo3, repository.Repository)
        self.assertEqual(nothing, None)
        
        # Pass in an invalid argument
        self.assertRaises(workbench.WorkBenchError, self.wb.init_repository, 52)
        
        
    @defer.inlineCallbacks
    def test_invalidate(self):
        
        repo, person = self.wb.init_repository(PERSON_TYPE)
        
        repo.commit('junk commit')
        
        # Checkout to invalidate it
        person_2 = yield repo.checkout('master')
        
        self.assertEqual(object.__getattribute__(person,'Invalid'), True)
        
    def test_wrapper_properties(self):
        """
        Test the basic state of a new wrapper object when it is created
        """
        repo, simple = self.wb.init_repository(ADDRESSBOOK_TYPE)   
        
        self.assertEqual(len(simple.ParentLinks),0)
        self.assertEqual(len(simple.ChildLinks),0)
        self.assertEqual(simple.IsRoot, True)
        self.assertEqual(simple.Modified, True)
        
        simple2= repo.create_object(ADDRESSLINK_TYPE)
        
        self.assertEqual(len(simple2.ParentLinks),0)
        self.assertEqual(len(simple2.ChildLinks),0)
        self.assertEqual(simple2.IsRoot, True)
        self.assertEqual(simple2.Modified, True)
        
        
    def test_inparents(self):
            
        repo, ab = self.wb.init_repository(ADDRESSLINK_TYPE)
        
        ab.person.add()
            
        ab2 = repo.create_object(ADDRESSLINK_TYPE)
            
        # Person is a null pointer - put an addresslink in it cause we can...
        ab.person[0] = ab2
            
        # add a link to the person list in the addresslink
        ab2.person.add()
        
        # Should fail due to circular reference!
        self.failUnlessRaises(repository.RepositoryError, ab2.person.SetLink, 0, ab)
        
        
    @defer.inlineCallbacks
    def test_branch_checkout(self):
        repo, ab = self.wb.init_repository(ADDRESSBOOK_TYPE)   
        p = ab.person.add()
        p.name = 'David'
        p.id = 1
        repo.commit()
        
        repo.branch(nickname="Arthur")
        
        self.assertEqual(p.name,'David')
        p.name = 'John'
        
        repo.commit()

        ab2 = yield repo.checkout(branchname="master")
        
        self.assertEqual(ab2.person[0].name, 'David')
        
        ab3 = yield repo.checkout(branchname="Arthur")
        
        self.assertEqual(ab3.person[0].name, 'John')
        
        
    def test_branch_no_commit(self):
        repo, ab = self.wb.init_repository(ADDRESSLINK_TYPE)
        self.assertEqual(len(repo.branches),1)
            
        self.assertRaises(repository.RepositoryError, repo.branch, 'Arthur')
        
    def test_branch(self):
        repo, ab = self.wb.init_repository(ADDRESSLINK_TYPE)
        repo.commit()
        self.assertEqual(len(repo.branches),1)
        
        # Create a branch
        branch_key = repo.branch("Arthur")   
        self.assertEqual(len(repo.branches),2)
        
        # Get by name
        branch = repo.get_branch('Arthur')
        self.assertEqual(branch.branchkey, branch_key)
        
        # Get by key
        branch = repo.get_branch(branch_key)
        self.assertEqual(branch.branchkey, branch_key)
        
        # delete
        repo.remove_branch('Arthur')
        self.assertEqual(len(repo.branches),1)

        
        
        
    def test_create_commit_ref(self):
        repo, ab = self.wb.init_repository(ADDRESSLINK_TYPE)
        cref = repo._create_commit_ref(comment="Cogent Comment")
        assert(cref.comment == "Cogent Comment")
            
    @defer.inlineCallbacks
    def test_checkout_commit_id(self):
        repo, ab = self.wb.init_repository(ADDRESSBOOK_TYPE)
        
        commit_ref1 = repo.commit()

        p = ab.person.add()
        p.id = 1
        p.name = 'Uma'
            
        commit_ref2 = repo.commit()
            
        p.name = 'alpha'
        commit_ref3 = repo.commit()
            
        ab = yield repo.checkout(branchname='master', commit_id=commit_ref1)
        self.assertEqual(len(ab.person),0)
            
        ab = yield repo.checkout(branchname='master', commit_id=commit_ref2)
        self.assertEqual(ab.person[0].id,1)
        self.assertEqual(ab.person[0].name,'Uma')
        
        ab = yield repo.checkout(branchname='master', commit_id=commit_ref3)
        self.assertEqual(ab.person[0].id,1)
        self.assertEqual(ab.person[0].name,'alpha')


    def test_error_on_set_linked_object(self):

        repo, ab = self.wb.init_repository(ADDRESSLINK_TYPE)

        self.assertRaises(repository.RepositoryError, setattr, ab , 'owner', 'anything but a gpbwrapper')




        
    def test_log(self):
        wb1 = workbench.WorkBench('No Process Test')
        
        repo1, ab = self.wb.init_repository(ADDRESSBOOK_TYPE)
        
        commit_ref1 = repo1.commit(comment='a')
        commit_ref2 = repo1.commit(comment='b')
        commit_ref3 = repo1.commit(comment='c')
            
            
        repo1.log_commits('master')
        
    @defer.inlineCallbacks  
    def test_dag_structure(self):
        repo, ab = self.wb.init_repository(ADDRESSLINK_TYPE)
                        
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
        
        self.assertIdentical(ab.person[0], ab.owner)
        
        cref = repo.commit(comment='testing commit')
        
        p = None
        ab = None
        
        ab = yield repo.checkout(branchname='master')

        
        self.assertEqual(len(ab.ChildLinks),3)
        
        self.assertEqual(ab.person[0], ab.owner)
        self.assertIdentical(ab.person[0], ab.owner)
        
        ab.owner.name = 'Michael'
        
        
        self.assertEqual(ab.owner.name, 'Michael')
        
        self.assertEqual(ab.person[0].name, 'Michael')

    @defer.inlineCallbacks
    def test_lost_objects(self):
        repo, ab = self.wb.init_repository(ADDRESSLINK_TYPE)
            
        # Create a resource object    
        p = repo.create_object(PERSON_TYPE)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '123 456 7890'
 
        ab.owner = p
        
        cref = repo.commit(comment='testing commit')
 
        ab2 = yield repo.checkout(branchname='master')
        
        # This is only for testing - to make sure that the invalid method is
        # working properly!
        self.assertEqual(object.__getattribute__(p,'Invalid'), True)
        self.assertEqual(object.__getattribute__(ab,'Invalid'), True)


    def test_transfer_repository_objects(self):
        
        repo1, ab1 = self.wb.init_repository(ADDRESSLINK_TYPE)
            
        # Create a resource object    
        p1 = repo1.create_object(PERSON_TYPE)
        p1.name='David'
        p1.id = 5
        p1.email = 'd@s.com'
        ph1 = p1.phone.add()
        ph1.type = p1.PhoneType.MOBILE
        ph1.number = '123 456 7890'
 
        ab1.owner = p1

        ab1.person.add()

        # Test copy to self = can't be done by assignment
        ab1.person[0] = repo1.copy_object(p1)
        self.assertNotIdentical(ab1.person[0],ab1.owner)
        self.assertNotEqual(ab1.person[0].MyId, ab1.owner.MyId)
        self.assertEqual(ab1.person[0], ab1.owner)

        ab1.person[0].name = 'John'

        cref = repo1.commit(comment='testing commit')


        # Create a second repository and copy p1 from repo1 to repo2
        repo2, ab2 = self.wb.init_repository(ADDRESSLINK_TYPE)
            
        ab2.person.add()
        
        # move to a repeated link by assignment
        ab2.person[0] = ab1.owner

        # Test the person
        self.assertEqual(ab2.person[0].name, 'David')
            
        self.assertNotEqual(ab2.person[0].MyId, ab1.owner.MyId)
        self.assertEqual(ab2.person[0], ab1.owner)
        self.assertNotIdentical(ab2.person[0], ab1.owner)
        self.assertNotIdentical(ab2.person[0].Repository, ab1.owner.Repository)
        
        self.assertIdentical(ab2.person[0].Repository, ab2.Repository)
        
        # move to a link by assignment - uses copy_object!
        ab2.owner = ab1.owner
        
        # Test the owner
        self.assertEqual(ab2.owner.name, 'David')
        self.assertNotEqual(ab2.owner.MyId, ab1.owner.MyId)
        self.assertEqual(ab2.owner, ab1.owner)
        self.assertNotIdentical(ab2.owner, ab1.owner)
        self.assertNotIdentical(ab2.owner.Repository, ab1.owner.Repository)
        
        
        
    @defer.inlineCallbacks 
    def test_merge(self):
        
        repo, ab = self.wb.init_repository(ADDRESSLINK_TYPE)
            
        # Create a resource object    
        p1 = repo.create_object(PERSON_TYPE)
        p1.name='David'
        p1.id = 5
        p1.email = 'd@s.com'
        ph1 = p1.phone.add()
        ph1.type = p1.PhoneType.WORK
        ph1.number = '123 456 7890'
        ab.owner = p1
        ab.person.add()
        ab.person[0] = p1
        
        ab.title = 'Junk'
        
        cref1 = repo.commit(comment='testing commit')
        
        repo.branch('Merge')
        
        # Create a resource object    
        p2 = repo.create_object(PERSON_TYPE)
        p2.name='John'
        p2.id = 3
        p2.email = 'J@G.com'
        ph2 = p1.phone.add()
        ph2.type = p1.PhoneType.WORK
        ph2.number = '098 765 4321'
        ab.person.add()
        ab.person[1] = p2
        
        cref2 = repo.commit('Appending a person')
        
        del ab, p1, p2, ph2, ph1
        
        ab = yield repo.checkout(branchname='master')

        yield repo.merge_with(branchname='Merge')


        self.assertEqual(ab.title, repo.merge[0].title)

        self.assertEqual(ab.person[0].name, repo.merge[0].person[0].name)
        
        self.assertNotIdentical(ab.person[0], repo.merge[0].person[0])
        
        # Can not modify merger objects   
        self.assertRaises(gpb_wrapper.OOIObjectError,setattr, repo.merge[0], 'title', 'David')
        self.assertRaises(gpb_wrapper.OOIObjectError,setattr, repo.merge[0].person[0], 'name', 'Matthew')
        
        # Can modify workspace objects
        ab.person[0].name = 'Matthew'
        ab.title = 'Not Junk!'
        
        # Can move object from merge to workspace
        ab.person.add()
        ab.person[1] = repo.merge[0].person[1]
        
        self.assertEqual(ab.person[1].name, 'John')
        
        # Commit and check history...
        cref3 = repo.commit('merge resolved')
        
        # Check commits...
        branch = repo._current_branch
        self.assertEqual(branch.commitrefs[0].MyId, cref3)
        self.assertEqual(branch.commitrefs[0].parentrefs[0].commitref.MyId, cref1)
        self.assertEqual(branch.commitrefs[0].parentrefs[1].commitref.MyId, cref2)
        


    def test_clear_repo(self):

        def closure(workbench):

            repo = self.wb.create_repository(ADDRESSLINK_TYPE)
            # Create a weakref proxy to the repository
            repo_ref = weakref.proxy(repo)

            return repo_ref

        repo1_ref = closure(self.wb)
        repo2_ref = closure(self.wb)

        # Check to make sure we can get to properties of both repositories
        self.assertEqual(repo1_ref.persistent, False)
        self.assertEqual(repo2_ref.persistent, False)
        # Neither one has been GCd because it is in the workbench.

        # Clear destroys the references too it.
        self.wb.clear_repository_key(repo1_ref.repository_key)

        # Make sure the test is deterministic - call garbage collection!
        gc.collect()

        # After clear the object is gone!
        self.assertRaises(ReferenceError, getattr, repo1_ref, 'persistent')
        self.assertEqual(repo2_ref.persistent, False)


        # Clear the second one
        self.wb.clear_repository_key(repo2_ref.repository_key)

        # Make sure the test is deterministic - call garbage collection!
        gc.collect()

        # After clear the object is gone!
        self.assertRaises(ReferenceError, getattr, repo2_ref, 'persistent')
        




    def test_clear(self):
        """
        In this test make weakrefs to a number of different objects in the repository and make sure all are GCd

        @TODO add a test to make sure that 
        """
        def closure(workbench):
            repo = workbench.create_repository(ADDRESSLINK_TYPE)

            # Create a resource object
            p1 = repo.create_object(PERSON_TYPE)
            p1.name='David'
            p1.id = 5
            p1.email = 'd@s.com'
            ph1 = p1.phone.add()
            ph1.type = p1.PhoneType.WORK
            ph1.number = '123 456 7890'
            repo.root_object.owner = p1
            repo.root_object.person.add()
            repo.root_object.person[0] = p1

            repo.root_object.title = 'Junk'

            cref1 = repo.commit(comment='testing commit')

            repo_refs=[]
            repo_refs.append(weakref.ref(repo))
            repo_refs.append(weakref.ref(repo.root_object.person[0]))
            repo_refs.append(weakref.ref(repo.root_object.owner))
            repo_refs.append(weakref.ref(repo.root_object.owner.GPBMessage))
            repo_refs.append(weakref.ref(repo.root_object.owner.phone[0].GPBMessage))
            repo_refs.append(weakref.ref(repo.root_object))
            repo_refs.append(weakref.ref(repo._current_branch))
            repo_refs.append(weakref.ref(repo._dotgit))

            for value in repo._commit_index.values():
                repo_refs.append(weakref.ref(value))

            for value in repo.index_hash.values():
                repo_refs.append(weakref.ref(value))


            return repo_refs


        repo_refs = closure(self.wb)

        for item in repo_refs:
            self.assertNotEqual(item(),None)

        self.wb.clear_non_persistent()

        gc.collect()

        for item in repo_refs:
            self.assertEqual(item(),None)
            
            
class MergeContainerTest(unittest.TestCase):
    
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

        repo.commit('Commit to serialize elements')
    

    def test_load_element(self):

        commit = self.repo._current_branch.commitrefs[0]

        mc = repository.MergeRepository(commit, self.repo.index_hash.cache)

        self.assertEqual(mc.root_object, self.ab)

    
    
    
    
    