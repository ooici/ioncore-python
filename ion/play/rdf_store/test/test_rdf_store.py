#!/usr/bin/env python

"""
@file ion/play/rdf_store/test/test_blob_store.py
@author Paul Hubbard
@author Michael Meisinger
@author David Stuebe
@brief test class for blob store
"""

import logging
from uuid import uuid4

from twisted.internet import defer

from ion.play.rdf_store.rdf_store import RdfStore
from ion.test.iontest import IonTestCase

from ion.play.rdf_store.rdf_base import RdfBlob, RdfAssociation, RdfEntity, RdfESBase, RdfState, WorkSpace


class RdfStoreTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.rdfs=RdfStore()
        yield self.rdfs.init()
        
        self.key = str(uuid4())
        self.blob=RdfBlob.create('fancy value!')


    @defer.inlineCallbacks
    def test_get_404(self):
        # Make sure we can't read the not-written
        rc = yield self.rdfs.blobs.get_blobs(self.key)
        self.failUnlessEqual(rc, [])
        
        rc = yield self.rdfs.associations.get_associations(self.key)
        self.failUnlessEqual(rc, [])
        
        rc = yield self.rdfs.states.get_key(self.key)
        self.failUnlessEqual(rc, None)
        
        rc = yield self.rdfs.a_refs.get_references(self.key)
        self.failUnlessEqual(rc, set())

    @defer.inlineCallbacks
    def test_commit_checkout_diff(self):
        
        triple1 = (RdfBlob.create('junk'),RdfBlob.create('in'),RdfBlob.create('trunk!'))
        assoc1=RdfAssociation.create(triple1[0],triple1[1],triple1[2])

        ws_in = WorkSpace.create([(assoc1,triple1)],self.key)
        
        print '===== Initial status ========'
        ws_in.print_status()
        
        yield self.rdfs.commit(ws_in)
        
        print '===== Committed status ========'
        ws_in.print_status()

        ws_out = yield self.rdfs.checkout(self.key)
        
        print '===== Checkout status ========'
        ws_out.print_status()
        

        # Add another associtation
        triple2 = (RdfBlob.create('junk'),RdfBlob.create('in'),RdfBlob.create('my trunk!'))
        ws_out.add_triple(triple2)

        print '===== Added new association status ========'
        ws_out.print_status()
        
        
        print '===== Print the Diff ========'
        ws_diff = yield self.rdfs.diff_commit(ws_out)
        ws_diff.print_status