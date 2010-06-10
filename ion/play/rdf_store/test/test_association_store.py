##!/usr/bin/env python
#
#"""
#@file ion/services/coi/test/test_resource_registry.py
#@author Michael Meisinger
#@brief test service for registering resources and client classes
#"""
#
#import logging
#logging = logging.getLogger(__name__)
#
#from twisted.internet import defer
#
#from ion.play.rdf_store.association_store import AssociationStore
#from ion.test.iontest import IonTestCase
#
#from ion.play.rdf_store.rdf_base import RdfBlob, RdfAssociation
#
#
#class AssociationTest(IonTestCase):
#    """Testing service classes of resource registry
#    """
#
#    @defer.inlineCallbacks
#    def setUp(self):
#        self.astore = AssociationStore()
#        yield self.astore.init()
#        
#        b1=RdfBlob.create('fancy value!1')
#        b2=RdfBlob.create('fancy value!2')
#        b3=RdfBlob.create('fancy value!3')
#        self.assoc=RdfAssociation.create(b1,b2,b3)
#
#    @defer.inlineCallbacks
#    def tearDown(self):
#        yield self.astore.delete_associations(self.assoc.key)
#        del self.astore
#        
#    @defer.inlineCallbacks
#    def test_get_404(self):
#        # Make sure we can't read the not-written
#        rc = yield self.astore.get_associations(self.assoc.key)
#        self.failUnlessEqual(rc, [])
#
#    @defer.inlineCallbacks
#    def test_write_and_delete(self):
#        # Hmm, simplest op, just looking for exceptions
#        yield self.astore.put_associations(self.assoc)
#
#    @defer.inlineCallbacks
#    def test_delete(self):
#        yield self.astore.put_associations(self.assoc)
#        yield self.astore.delete_associations(self.assoc.key)
#        rc = yield self.astore.get_associations(self.assoc.key)
#        self.failUnlessEqual(rc, [])
#
#    @defer.inlineCallbacks
#    def test_put_get_delete(self):
#        # Write, then read to verify same
#        yield self.astore.put_associations(self.assoc)
#        b = yield self.astore.get_associations(self.assoc.key)
#        self.failUnlessEqual([self.assoc], b)