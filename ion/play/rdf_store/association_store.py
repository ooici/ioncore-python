#!/usr/bin/env python

"""
@file ion/play/rdf_store/association_service.py
@author David Stuebe
@brief  RDF Store: association service
"""

import logging
from twisted.internet import defer

from ion.data.store import Store, IStore
from ion.play.rdf_store.rdf_base import RdfAssociation, RdfMixin

class AssociationStore(object):
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
        logging.info("AssociationStore initialized")


    @defer.inlineCallbacks
    def put_associations(self, associations):
        '''
        '''
        if not getattr(associations, '__iter__', False):
            associations = (associations,)

        for association in associations:
            assert isinstance(association, RdfAssociation)
            rc=yield self.store.put(association.key,association.object)

        defer.returnValue(rc)




    @defer.inlineCallbacks
    def read_state(self, state):
        assert isinstance(state, RdfMixin)
        
        associations = yield self.get_associations(state.object)
        defer.returnValue(associations)

    @defer.inlineCallbacks
    def get_associations(self, keys):
        '''
        '''
        if not getattr(keys, '__iter__', False):
            keys = (keys,)
        
        associations=[]
        #@ How to make this asynchronis?
        for key in keys:
            
            # an association returns a tuple - key,commit
            if type(key) is tuple:
                key = key[0]
                
            association = yield self.store.get(key)
            if association:
                association = RdfAssociation.load(key,association)
                associations.append(association)
            else:
                logging.debug("Key not found in AssociationStore!")                    

        defer.returnValue(associations)

    @defer.inlineCallbacks
    def delete_associations(self, keys):
        '''
        '''
        if not getattr(keys, '__iter__', False):
            keys = (keys,)

        for key in keys:           
            rc=yield self.store.remove(key)

        defer.returnValue(rc)


