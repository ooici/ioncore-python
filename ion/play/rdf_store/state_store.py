#!/usr/bin/env python

"""
@file ion/play/rdf_store/blob_service.py
@author David Stuebe
@brief  RDF Store: blob service
"""

import logging
from twisted.internet import defer

from ion.data.objstore import ObjectStore, ValueObject
from ion.data.store import IStore, Store
from ion.data.dataobject import DataObject

from ion.play.rdf_store.rdf_base import RdfState, RdfEntity, RdfESBase


class StateStore(object):
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
        self.objstore = ObjectStore(backend=None, backargs=None)

    #@TODO make this also a class method so it is easier to start - one call?
    @defer.inlineCallbacks
    def init(self):
        """
        Initializes the ObjectStore class
        @retval Deferred
        """
        yield self.objstore.init()
        logging.info("StateStore initialized")
        
    @defer.inlineCallbacks
    def put_states(self, states):
        '''
        '''
        if not getattr(states, '__iter__', False):
            states = (states,)
        
        key_commit_list=[]
        for state in states:
        
            if not getattr(state, 'commitRefs', False):
                parents=state.commitRefs
            else:
                parents=None
        
            obj = ValueObject(list(state.object))
        
            rc=yield self.objstore.put(state.key, obj, parents=parents)
        
        key_commit_list.append((state.key,rc.identity))

        defer.returnValue(key_commit_list)
        

    @defer.inlineCallbacks
    def get_key(self, key,commit=None):
        '''
        '''
            
        # if we got a commit to fetch, make sure we got a single key - usually a single stre in a list...
        if commit:
            if getattr(commit, '__iter__', False):
                assert len(commit)==1
                commit = commit[0]
                    
                
        dobj=yield self.objstore.get(key, commit=commit)
        

        if dobj:
            state=RdfState.load(key,set(dobj.value),[dobj.commitRef])
        else:
            logging.info("StateStore Key/Commit Not Found!")
            state=None

        defer.returnValue(state)


    @defer.inlineCallbacks
    def get_states(self, stateRefs):
        '''
        '''
            
        if not getattr(stateRefs, '__iter__', False):
            stateRefs = (stateRefs,)
        
        states=[]
        for stateRef in stateRefs:
            if isinstance(stateRef, RdfESBase):
                key = stateRef.key
                commit = stateRef.commitRefs
            elif isinstance(stateRef, tuple):
                key = stateRef[0]
                commit=stateRef[1]
                
            state = yield self.get_key(key,commit)
            if state:
                states.append(state)

        defer.returnValue(states)

