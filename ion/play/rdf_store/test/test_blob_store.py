#!/usr/bin/env python

"""
@file ion/play/rdf_store/test/test_blob_store.py
@author Paul Hubbard
@author Michael Meisinger
@author David Stuebe
@brief test class for blob store
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer

from ion.play.rdf_store.blob_store import BlobStore
from ion.test.iontest import IonTestCase

from ion.play.rdf_store.rdf_base import RdfBlob


class BlobTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.bstore=BlobStore()
        yield self.bstore.init()
        
        self.blob=RdfBlob.create('fancy value!')

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.bstore.delete_blobs(self.blob.key)
        del self.bstore
        
    @defer.inlineCallbacks
    def test_get_404(self):
        # Make sure we can't read the not-written
        rc = yield self.bstore.get_blobs(self.blob.key)
        self.failUnlessEqual(rc, [])

    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.bstore.put_blobs(self.blob)

    @defer.inlineCallbacks
    def test_delete(self):
        yield self.bstore.put_blobs(self.blob)
        yield self.bstore.delete_blobs(self.blob.key)
        rc = yield self.bstore.get_blobs(self.blob.key)
        self.failUnlessEqual(rc, [])

    @defer.inlineCallbacks
    def test_put_get_delete(self):
        # Write, then read to verify same
        yield self.bstore.put_blobs(self.blob)
        b = yield self.bstore.get_blobs(self.blob.key)
        self.failUnlessEqual([self.blob], b)