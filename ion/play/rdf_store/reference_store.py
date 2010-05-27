#!/usr/bin/env python

"""
@file ion/play/rdf_store/reference_service.py
@author David Stuebe
@brief  RDF Store: reference service
"""

import logging
from twisted.internet import defer

from ion.data.set_store import SetStore, ISetStore

class ReferenceStore(object):
    """Example service implementation
    """
    def __init__(self, backend=None, backargs=None):
        """
        @param backend  Class object with a compliant Store or None for memory
        @param backargs arbitrary keyword arguments, for the backend
        """
        self.backend = backend if backend else SetStore
        self.backargs = backargs if backargs else {}
        assert issubclass(self.backend, ISetStore)
        assert type(self.backargs) is dict

        # KVS with value ID -> value
        self.setstore = None

    #@TODO make this also a class method so it is easier to start - one call?
    @defer.inlineCallbacks
    def init(self):
        """
        Initializes the ValueStore class
        @retval Deferred
        """
        self.setstore = yield self.backend.create_store(**self.backargs)
        logging.info("Reference Store (Sets) initialized")

    @defer.inlineCallbacks
    def add_references(self, key, references):
        '''
        '''
        if not getattr(references, '__iter__', False):
            references = (references,)

        for ref in references:
            rc=yield self.setstore.sadd(key,ref)
        # @Todo What do I do with multiple returns from a yield?

        defer.returnValue(rc)
        
    @defer.inlineCallbacks
    def remove_references(self, key, references):
        '''
        '''
        if not getattr(references, '__iter__', False):
            references = (references,)

        for ref in references:
            rc = yield self.setstore.sremove(key,ref)
            
        defer.returnValue(rc)


    @defer.inlineCallbacks
    def get_references(self, key):
        '''
        '''
        rc = yield self.setstore.smembers(key)
            
        defer.returnValue(rc)


    @defer.inlineCallbacks
    def delete(self, key):
        rc=yield self.setstore.remove(key)
        defer.returnValue(rc)


