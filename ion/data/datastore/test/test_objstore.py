#!/usr/bin/env python
"""
@file ion/data/datastore/test/test_objstore.py
@author Michael Meisinger
@author Dorian Raymer
@brief test object store
"""

import logging

from twisted.internet import defer
from twisted.trial import unittest

from ion.data import store
from ion.data.datastore import cas
from ion.data.datastore import objstore

from ion.data import resource

sha1 = cas.sha1

class ObjectStoreTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.backend_store = yield store.Store.create_store()
        self.test_namespace = 'test_namespace'
        self.object_store = yield objstore.ObjectStore.new(self.backend_store, self.test_namespace)
        
        

    @defer.inlineCallbacks
    def test_new_fail(self):
        try:
            object_store2 = yield objstore.ObjectStore.new(self.backend_store, self.test_namespace)
            self.fail()
        except objstore.ObjectStoreError:
            pass

    @defer.inlineCallbacks
    def test_create_object(self):
        obj = yield self.object_store.create('thing',resource.IdentityResource)
        self.assert_(isinstance(obj, objstore.ObjectChassis))

    @defer.inlineCallbacks
    def test_load_store(self):
        object_store2 = yield self.object_store.load(self.backend_store, self.test_namespace)
        


class ObjectChassisTest(unittest.TestCase):
    """
    @TODO
    Test:
        Commit
        Checkout
        get_commit_history
        new
        
    How can we make these more like unit tests and less like scripts?
    """
    pass


    




#class ObjectStoreTest(unittest.TestCase):
#
#    @defer.inlineCallbacks
#    def (self):
#        """
#        Test the store mechanics with the in-memory Store backend.
#        """
#        self.name = 'test_namespace'
#        backend_store = yield store.Store.create_store()
#        self.objstore = yield objstore.ObjectStore.new(backend_store, self.name)
#
#
#
#    def tearDown(self):
#        """
#        @note This raises a good point for the IStore interface:
#            - Namespaceing
#            - removing recursively
#            - removing a pattern
#        """
#
#    @defer.inlineCallbacks
#    def test_create_object(self):
#        obj = yield self.objstore.create('thing',resource.IdentityResource)
#
#
#
#
#
#
#    @defer.inlineCallbacks
#    def xtest_make_tree(self):
#        """
#        @brief Rough script for development; Not a unit.
#        """
#
#        b =  cas.Blob('test content')
#        b2 =  cas.Blob('deja vu')
#        b3 =  cas.Blob('jamais vu')
#        yield self.cas.put(b)
#        yield self.cas.put(b2)
#        yield self.cas.put(b3)
#        t1 = cas.Tree(cas.Entity('test', sha1(b)),
#                            cas.Entity('hello', sha1(b2)))
#        t1id = yield self.cas.put(t1)
#        t2 = cas.Tree(cas.Entity('thing', sha1(b3)),
#                            cas.Entity('tree', sha1(t1)))
#        t2id = yield self.cas.put(t2)
#        c = cas.Commit(t2id, log='first commit')
#        cid = yield self.cas.put(c)
#        c_out = yield self.cas.get(cid)
#        b3new = cas.Blob('I remember, now!')
#        b3newid = yield self.cas.put(b3new)
#
#        t2new = cas.Tree(cas.Entity('thing', sha1(b3new)),
#                            cas.Entity('tree', sha1(t1)))
#        t2newid = yield self.objstore.put(t2new)
#        cnew = cas.Commit(t2newid, parents=[cid], log='know what i knew but forgot')
#        cnewid = yield self.objstore.put(cnew)
#        yield self.objstore.update_ref(cnewid)
#
#        wt = yield self.objstore.checkout()
#        yield wt.load_objects()


