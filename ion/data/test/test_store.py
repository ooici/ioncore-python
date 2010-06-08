#!/usr/bin/env python

"""
@file ion/data/backends/test/test_cassandra.py
@author Paul Hubbard
@author Dorian Raymer
@author David Stuebe
@test Service test of IStore Implementation
"""

import logging
logging = logging.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
from twisted.internet import defer

from ion.data.backends import cassandra
from ion.data.store import Store


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
        d = Store.create_store()
        return d

    #@defer.inlineCallbacks
    def tearDown(self):
        #yield self.ds.remove(self.key)
        #del self.ds #shouldn't need
        pass

    @defer.inlineCallbacks
    def test_get_404(self):
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

    @defer.inlineCallbacks
    def test_query(self):
        # Write a key, query for it, verify contents
        yield self.ds.put(self.key, self.value)
        rl = yield self.ds.query(self.key)
#        print 'type rl',type(rl)
#        print 'type rl[0]', type(rl[0])
#        print 'type rl[0][0]', type(rl[0][0])
        self.failUnlessEqual(rl[0][0], self.key)



class CassandraStoreTest(IStoreTest):

    def _setup_backend(self):
        clist = ['amoeba.ucsd.edu:9160']
        d = cassandra.CassandraStore.create_store(cass_host_list=clist)
        return d

