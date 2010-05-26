#!/usr/bin/env python

"""
@file ion/play/rdf_store/test/test_blob_store.py
@author Paul Hubbard
@author Michael Meisinger
@author David Stuebe
@brief test class for state store using the objectstore as a backend
"""

import logging

from twisted.internet import defer

from ion.play.rdf_store.state_store import StateStore
from ion.test.iontest import IonTestCase

from ion.play.rdf_store.rdf_base import RdfEntity, RdfState, RdfAssociation


class StateTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.states=StateStore()
        yield self.states.init()
        
        self.assoc1 = RdfAssociation.load('key1',{'a':'dict'})
        self.assoc2 = RdfAssociation.load('key2',{'a':'dict'})

        self.entity1=RdfEntity.create(self.assoc1)

#    @defer.inlineCallbacks
    def tearDown(self):
        del self.states
        
    @defer.inlineCallbacks
    def test_get_404(self):
        # Make sure we can't read the not-written
        print 'hello1'
        rc = yield self.states.get_state(RdfEntity.create(None))
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.states.put_state(self.entity1)
    

    @defer.inlineCallbacks
    def test_put_get(self):
        # Write, then read to verify same
        yield self.states.put_state(self.entity1)
        e = yield self.states.get_state(RdfEntity.create(None,self.entity1.key))
        self.failUnlessEqual(self.entity1, e)
        
        
        
        
        