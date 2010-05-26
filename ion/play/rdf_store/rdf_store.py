#!/usr/bin/env python


import logging
from twisted.internet import defer

from ion.data.store import Store, IStore
from ion.data.set_store import SetStore, ISetStore

from ion.play.rdf_store.rdf_base import RdfBlob, RdfAssociation, RdfBase, RdfEntity, RdfState, WorkSpace

class RdfStore(object):

    def __init__(self, store_be=None, store_bea=None, set_be=None, set_bea=None):
        """
        @param store_be  Class object with a compliant Store or None for memory
        @param store_bea arbitrary keyword arguments, for the backend
        @param set_be  Class object with a compliant Set Store or None for memory
        @param set_bea arbitrary keyword arguments, for the backend
        """
        self.store_be = store_be if store_be else Store
        self.store_bea = store_bea if store_bea else {}
        assert issubclass(self.store_be, IStore)
        assert type(self.store_bea) is dict

        self.set_be = set_be if set_be else SetStore
        self.set_bea = set_bea if set_bea else {}
        assert issubclass(self.store_be, ISetStore)
        assert type(self.store_bea) is dict


        #Declare the stores
        self.blobs = BlobStore(backend=store_be,backendargs=store_bea)
        self.associations=AssociationStore(backend=store_be,backendargs=store_bea)
        self.state=EntityStore(backend=store_be,backendargs=store_bea)
        #Declare the Set Stores
        self.a_refs=ReferenceStore(backend=set_be,backendargs=set_bea)
        self.e_refs=ReferenceStore(backend=set_be,backendargs=set_bea)
        
        
    #@TODO make this also a class method so it is easier to start - one call?
    @defer.inlineCallbacks
    def init(self):
        """
        Initializes the ValueStore class
        @retval Deferred
        """
        yield self.blobs.init()
        yield self.associations.init()
        yield self.states.init()
        yield self.a_refs.init()
        yield self.e_refs.init()
        
    def checkout(self,key):
        # calls to repos...
        # pass result to WorkSpace.load()
        # service will pass the whole workspace for now
        return WorkSpace()
        
        
        
    def commit(self,workspace):
        # Commit a workspace to the repository
        # Service will pass the whole workspace for now!
        pass
    
    # To be implemented later! Make it distributed so services can work locally!
    def push(self,key,**kwargs):
        pass
    
    def pull(self,key,**kwargs):
        pass
    
    
    
        
        
        
    
            
        
        
        
        
        
        
        
        
        
        
            
            