#!/usr/bin/env python

"""
@file ion/play/rdf_store/blob_service.py
@author David Stuebe
@brief  RDF Store: blob service
"""

import logging
from twisted.internet import defer

from ion.data.store import Store, IStore

from ion.play.rdf_store.rdf_base import RdfBlob


class BlobStore(object):
    """Example service implementation
    """
    def __init__(self, backend=None, backargs=None):
        """
        @param backend  Class object with a compliant Store or None for memory
        @param backargs arbitrary keyword arguments, for the backend
        """
        self.backend = backend if backend else Store
        self.backargs = backargs if backargs else {}
        assert issubclass(self.backend, IStore)
        assert type(self.backargs) is dict

        # KVS with value ID -> value
        self.store = None

    #@TODO make this also a class method so it is easier to start - one call?
    @defer.inlineCallbacks
    def init(self):
        """
        Initializes the ValueStore class
        @retval Deferred
        """
        self.store = yield self.backend.create_store(**self.backargs)
        logging.info("BlobStore initialized")
        
    @defer.inlineCallbacks
    def put_blob(self, blob):
        '''
        '''
        assert isinstance(blob, RdfBlob)
        rc=yield self.store.put(blob.key, blob.object)
        defer.returnValue(rc)
        

    @defer.inlineCallbacks
    def get_blob(self, key):
        '''
        '''
        blob = yield self.store.get(key)
        if blob:
            blob = RdfBlob.load(key,blob)
        defer.returnValue(blob)

    @defer.inlineCallbacks
    def delete_blob(self, key):
        '''
        '''
        rc=yield self.store.delete(key)
        defer.returnValue(rc)


