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


    #@defer.inlineCallbacks
    #def test_get_404(self):
    #    # Make sure we can't read the not-written
    #    rc = yield self.rdfs.blobs.get_blobs(self.key)
    #    self.failUnlessEqual(rc, [])
    #    
    #    rc = yield self.rdfs.associations.get_associations(self.key)
    #    self.failUnlessEqual(rc, [])
    #    
    #    rc = yield self.rdfs.states.get_key(self.key)
    #    self.failUnlessEqual(rc, None)
    #    
    #    rc = yield self.rdfs.a_refs.get_references(self.key)
    #    self.failUnlessEqual(rc, set())

    def _compare_ws(self,ws1,ws2):
        self.assertEqual(ws1.get_associations(),ws2.get_associations())
        self.assertEqual(ws1.get_blobs(),ws2.get_blobs())
        self.assertEqual(ws1.get_entities(),ws2.get_entities())
        self.assertEqual(ws1.get_states(),ws2.get_states())
        self.assertEqual(ws1.key,ws2.key)
        self.assertEqual(ws1.get_references(),ws2.get_references())
        self.assertEqual(ws1.commitRefs,ws2.commitRefs)

    @defer.inlineCallbacks
    def test_commit_checkout_diff(self):
        
        triple1 = (RdfBlob.create('junk'),RdfBlob.create('in'),RdfBlob.create('trunk!'))
        assoc1=RdfAssociation.create(triple1[0],triple1[1],triple1[2])

        ws1_in = WorkSpace.create([(assoc1,triple1)],self.key)
        
        print '===== Initial status ========'
        ws1_in.print_status()
        
        yield self.rdfs.commit(ws1_in)
        
        print '===== Committed ws1 status ========'
        ws1_in.print_status()

        ws1_out = yield self.rdfs.checkout(self.key)
        
        self._compare_ws(ws1_out,ws1_in)

        print '===== Checkout latest ========'
        ws1_out.print_status()
        

        ws2_in = ws1_out.copy()
        # Add another associtation
        triple2 = (RdfBlob.create('junk'),RdfBlob.create('in'),RdfBlob.create('my trunk!'))
        ws2_in.add_triple(triple2)

        print '===== Added new association status ========'
        ws2_in.print_status()
        
        
        print '===== Print the Diff ========'
        ws_diff = yield self.rdfs.diff_commit(ws2_in)
        ws_diff.print_status()
        
        
        yield self.rdfs.commit(ws2_in)
        print '===== Committed ws2 status ========'
        ws2_in.print_status()

        ws2_out = yield self.rdfs.checkout(self.key)

        self._compare_ws(ws2_out,ws2_in)

        print '===== Checkout ws1 ========'
        ws1_a = yield self.rdfs.checkout(ws1_in.key,ws1_in.commitRefs)
        ws1_a.print_status()

        self._compare_ws(ws1_a,ws1_out)

        print '===== Print the Diff against ws1 commit ========'
        ws_diff = yield self.rdfs.diff_commit(ws2_in, ws1_in.commitRefs)
        ws_diff.print_status()
        
        print '===== Complete ========'