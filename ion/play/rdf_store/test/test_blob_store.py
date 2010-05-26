#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging

from twisted.internet import defer

from ion.play.rdf_store.blob_store import BlobStore
from ion.test.iontest import IonTestCase

from ion.play.rdf_store.rdf_base import RdfBlob


class BlobTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.bsc=BlobStore()
        yield self.bsc.init()
        
        self.blob=RdfBlob.create('fancy value!')

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.bsc.delete_blobs(self.blob.key)
        del self.bsc
        
    @defer.inlineCallbacks
    def test_get_404(self):
        # Make sure we can't read the not-written
        rc = yield self.bsc.get_blobs(self.blob.key)
        self.failUnlessEqual(rc, [])

    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.bsc.put_blobs(self.blob)

    @defer.inlineCallbacks
    def test_delete(self):
        yield self.bsc.put_blobs(self.blob)
        yield self.bsc.delete_blobs(self.blob.key)
        rc = yield self.bsc.get_blobs(self.blob.key)
        self.failUnlessEqual(rc, [])

    @defer.inlineCallbacks
    def test_put_get_delete(self):
        # Write, then read to verify same
        yield self.bsc.put_blobs(self.blob)
        b = yield self.bsc.get_blobs(self.blob.key)
        self.failUnlessEqual([self.blob], b)