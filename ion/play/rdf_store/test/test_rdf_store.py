#!/usr/bin/env python

"""
@file ion/play/rdf_store/test/test_blob_store.py
@author Paul Hubbard
@author Michael Meisinger
@author David Stuebe
@brief test class for blob store
"""

import logging

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
        
        self.blob=RdfBlob.create('fancy value!')

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.rdfs.delete_blobs(self.blob.key)
        del self.rdfs
        
    @defer.inlineCallbacks
    def test_get_404(self):
        # Make sure we can't read the not-written
        rc = yield self.bstore.get_blobs(self.blob.key)
        self.failUnlessEqual(rc, [])
