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
    def put_blobs(self, blobs):
        '''
        '''
        if not getattr(blobs, '__iter__', False):
            blobs = (blobs,)

        for blob in blobs:
            assert isinstance(blob, RdfBlob)
            rc=yield self.store.put(blob.key, blob.object)
        # @Todo What do I do with multiple returns from a yield?
        defer.returnValue(rc)
        

    @defer.inlineCallbacks
    def get_blobs(self, keys):
        '''
        '''
        if not getattr(keys, '__iter__', False):
            keys = (keys,)
        
        blobs=[]
        #@ How to make this asynchronis?
        for key in keys:
            blob = yield self.store.get(key)
            if blob:
                blob = RdfBlob.load(key,blob)
                blobs.append(blob)
            else:
                logging.debug("Key not found in BlobStore!")                    

        defer.returnValue(blobs)

    @defer.inlineCallbacks
    def delete_blobs(self, keys):
        '''
        '''
        if not getattr(keys, '__iter__', False):
            keys = (keys,)

        for key in keys:           
            rc=yield self.store.remove(key)

        defer.returnValue(rc)


