#!/usr/bin/env python

"""
@file ion/data/test/test_store.py
@author Paul Hubbard
@author Dorian Raymer
@author David Stuebe
@author Matt Rodriguez
@test Service test of IStore Implementation
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
from twisted.internet import defer
from twisted.internet import reactor

from ion.core.data import store
from ion.core.data import cassandra

from ion.test.iontest import IonTestCase


class IStoreTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.ds = yield self._setup_backend()
        self.key = str(uuid4())
        self.value = str(uuid4())

    def _setup_backend(self):
        """return a deferred which returns a initiated instance of a
        backend
        """
        return defer.maybeDeferred(store.Store)

    @defer.inlineCallbacks
    def test_get_none(self):
        # Make sure we can't read the not-written
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)

    @defer.inlineCallbacks
    def test_delete(self):
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_put_get_delete(self):
        # Write, then read to verify same
        yield self.ds.put(self.key, self.value)
        b = yield self.ds.get(self.key)
        self.failUnlessEqual(self.value, b)
        yield self.ds.remove(self.key)


class CassandraStoreTest(IStoreTest):

    def _setup_backend(self):
        #host = 'amoeba.ucsd.edu'
        host = 'localhost'
        #host = 'ec2-204-236-159-249.us-west-1.compute.amazonaws.com'
        port = 9160
        namespace = 'iontest'
        builder = cassandra.CassandraFactory(host, port, reactor)
        store = builder.buildStore(namespace)
        store.namespace = namespace
        
        manager_builder = cassandra.CassandraManagerFactory(host, port, reactor)
        cas_man = manager_builder
        #cas_man.create(namespace)
        
        
        return defer.succeed(store)

    def tearDown(self):
        #cas_man = cassandra.CassandraManager(self.ds.client)
        #cas_man.remove(self.ds.namespace)
        
        
        self.ds.client.manager.shutdown()


