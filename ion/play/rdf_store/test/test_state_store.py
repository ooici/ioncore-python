##!/usr/bin/env python
#
#"""
#@file ion/play/rdf_store/test/test_blob_store.py
#@author Paul Hubbard
#@author Michael Meisinger
#@author David Stuebe
#@brief test class for state store using the objectstore as a backend
#"""
#
#import logging
#logging = logging.getLogger(__name__)
#
#from twisted.internet import defer
#
#from ion.play.rdf_store.state_store import StateStore
#from ion.test.iontest import IonTestCase
#
#from ion.play.rdf_store.rdf_base import RdfEntity, RdfState, RdfAssociation
#
#
#class StateTest(IonTestCase):
#    """Testing service classes of resource registry
#    """
#
#    @defer.inlineCallbacks
#    def setUp(self):
#        self.states=StateStore()
#        yield self.states.init()
#        
#        # make some associations with dummy payload
#        self.assoc1 = RdfAssociation.load('key1',{'a':'dict'})
#        self.assoc2 = RdfAssociation.load('key2',{'a':'dict'})
#
#        self.entity1=RdfEntity.create(self.assoc1)
#
##    @defer.inlineCallbacks
#    def tearDown(self):
#        del self.states
#        
#    @defer.inlineCallbacks
#    def test_get_404(self):
#        # Make sure we can't read the not-written
#        rc = yield self.states.get_key('random key not in store')
#        self.failUnlessEqual(rc, None)
#
#    @defer.inlineCallbacks
#    def test_write_and_delete(self):
#        # Hmm, simplest op, just looking for exceptions
#        yield self.states.put_states(self.entity1)
#    
#
#    @defer.inlineCallbacks
#    def test_put_get(self):
#        # Write, then read to verify same
#        
#        # Write a state
#        rc=yield self.states.put_states(self.entity1)
#        print 'commited',rc
#        
#        # Read it back and check the object contents
#        s1 = yield self.states.get_states(self.entity1)
#        s1=s1[0]
#        self.assertEqual(self.entity1.object, s1.object)
#        
#        # add an new association to the state and put it back again
#        s1.add(self.assoc2)
#        rc=yield self.states.put_states(s1)
#        print 'commited',rc
#
#        # Get the new one back
#        s2 = yield self.states.get_key(self.entity1.key)
#        self.assertEqual(s2.object, s1.object)
#        
#        # Get the old one back!
#        s3=yield self.states.get_key(s1.key, s1.commitRefs)
#        self.assertEqual(s3.object,self.entity1.object)
#        
#        
#        
#        
#        