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

invalid_type = object_utils.create_type_identifier(object_id=-1, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)


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
        
        
    @defer.inlineCallbacks
    def test_invalidate(self):
        
        repo, person = self.wb.init_repository(person_type)
        
        repo.commit('junk commit')
        
        # Checkout to invalidate it
        person_2 = yield repo.checkout('master')
        
        self.assertEqual(object.__getattribute__(person,'Invalid'), True)
        
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
        
        
    def test_inparents(self):
            
        repo, ab = self.wb.init_repository(addresslink_type)
        
        ab.person.add()
            
        ab2 = repo.create_object(addresslink_type)
            
        # Person is a null pointer - put an addresslink in it cause we can...
        ab.person[0] = ab2
            
        # add a link to the person list in the addresslink
        ab2.person.add()
        
        # Should fail due to circular reference!
        self.failUnlessRaises(repository.RepositoryError, ab2.person.SetLink, 0, ab)
        
        
    @defer.inlineCallbacks
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

        ab2 = yield repo.checkout(branchname="master")
        
        self.assertEqual(ab2.person[0].name, 'David')
        
        ab3 = yield repo.checkout(branchname="Arthur")
        
        self.assertEqual(ab3.person[0].name, 'John')
        
        
    def test_branch_no_commit(self):
        repo, ab = self.wb.init_repository(addresslink_type)
        self.assertEqual(len(repo.branches),1)
            
        self.assertRaises(repository.RepositoryError, repo.branch, 'Arthur')
        
    def test_branch(self):
        repo, ab = self.wb.init_repository(addresslink_type)
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
        repo, ab = self.wb.init_repository(addresslink_type)
        cref = repo._create_commit_ref(comment="Cogent Comment")
        assert(cref.comment == "Cogent Comment")
            
    @defer.inlineCallbacks
    def test_checkout_commit_id(self):
        repo, ab = self.wb.init_repository(addressbook_type)
        
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
        
        
    def test_log(self):
        wb1 = workbench.WorkBench('No Process Test')
        
        repo1, ab = self.wb.init_repository(addressbook_type)
        
        commit_ref1 = repo1.commit(comment='a')
        commit_ref2 = repo1.commit(comment='b')
        commit_ref3 = repo1.commit(comment='c')
            
            
        repo1.log_commits('master')
        
    @defer.inlineCallbacks  
    def test_dag_structure(self):
        repo, ab = self.wb.init_repository(addresslink_type)
                        
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
        repo, ab = self.wb.init_repository(addresslink_type)
            
        # Create a resource object    
        p = repo.create_object(person_type)
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
        
        repo1, ab1 = self.wb.init_repository(addresslink_type)
            
        # Create a resource object    
        p1 = repo1.create_object(person_type)
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
        repo2, ab2 = self.wb.init_repository(addresslink_type)
            
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
        
        repo, ab = self.wb.init_repository(addresslink_type)
            
        # Create a resource object    
        p1 = repo.create_object(person_type)
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
        p2 = repo.create_object(person_type)
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
        
        yield repo.merge(branchname='Merge')
        
        self.assertEqual(ab.title, repo.merge_objects[0].title)
        self.assertEqual(ab.person[0].name, repo.merge_objects[0].person[0].name)
        
        self.assertNotIdentical(ab.person[0], repo.merge_objects[0].person[0])
        
        # Can not modify merger objects   
        self.assertRaises(gpb_wrapper.OOIObjectError,setattr, repo.merge_objects[0], 'title', 'David')
        self.assertRaises(gpb_wrapper.OOIObjectError,setattr, repo.merge_objects[0].person[0], 'name', 'Matthew')
        
        # Can modify workspace objects
        ab.person[0].name = 'Matthew'
        ab.title = 'Not Junk!'
        
        # Can move object from merge to workspace
        ab.person.add()
        ab.person[1] = repo.merge_objects[0].person[1]
        
        self.assertEqual(ab.person[1].name, 'John')
        
        # Commit and check history...
        cref3 = repo.commit('merge resolved')
        
        # Check commits...
        branch = repo._current_branch
        self.assertEqual(branch.commitrefs[0].MyId, cref3)
        self.assertEqual(branch.commitrefs[0].parentrefs[0].commitref.MyId, cref1)
        self.assertEqual(branch.commitrefs[0].parentrefs[1].commitref.MyId, cref2)
        
        


    def test_clear(self):

        repo = self.wb.create_repository(addresslink_type)

        # Create a resource object
        p1 = repo.create_object(person_type)
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
        

        person_ref = weakref.proxy(repo.root_object.person[0])
        addressbook_ref = weakref.proxy(repo.root_object)

        self.assertEqual(addressbook_ref.title, 'Junk')
        self.assertEqual(person_ref.name, 'David')


        repo.clear()

        gc.collect()

        referrers = gc.get_referrers(person_ref, addressbook_ref)

        print 'Referrers', referrers


        self.assertRaises(ReferenceError, getattr, addressbook_ref, 'title')

        self.assertRaises(ReferenceError, getattr, person_ref, 'name')




 
 
 