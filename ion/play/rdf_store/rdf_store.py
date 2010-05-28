#!/usr/bin/env python


import logging
from twisted.internet import defer

from ion.data.store import Store, IStore
from ion.data.set_store import SetStore, ISetStore

from ion.play.rdf_store.rdf_base import RdfBlob, RdfAssociation, RdfBase, RdfEntity, RdfState, WorkSpace, RdfMixin

from ion.play.rdf_store.state_store import StateStore
from ion.play.rdf_store.association_store import AssociationStore
from ion.play.rdf_store.blob_store import BlobStore
from ion.play.rdf_store.reference_store import ReferenceStore



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
        assert issubclass(self.set_be, ISetStore)
        assert type(self.set_bea) is dict


        #Declare the stores
        self.blobs = BlobStore(backend=store_be,backargs=store_bea)
        self.associations=AssociationStore(backend=store_be,backargs=store_bea)
        self.states=StateStore(backend=store_be,backargs=store_bea)
        #Declare the Set Stores
        self.a_refs=ReferenceStore(backend=set_be,backargs=set_bea)
        self.e_refs=ReferenceStore(backend=set_be,backargs=set_bea)
        
        
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
        
    @defer.inlineCallbacks
    def checkout_state(self,state):
        assert isinstance(state, RdfMixin)

        # if it is just a reference to the state, get the state
        if not state.object:
            state = yield self.states.get_states(state)
        
        # sort the associations into blobs, states, associations and entities
        
        aset=yield self.associations.read_state(state)
        
        sortedkeys=RdfAssociation.sort_keys(aset)

        # Get the blobs
        #print 'sortedkeys',sortedkeys
        bset= yield self.blobs.get_blobs(sortedkeys[RdfBase.BLOB])
        
        sset=[]
        for key_commit in sortedkeys[RdfBase.STATE]:
            sset.append( RdfState.reference(key_commit[0],key_commit[1]) )
            
        #sset= yield self.states.get_states(sortedkeys[RdfBase.STATE])
        
        eset=[]
        for key_commit in sortedkeys[RdfBase.ENTITY]:
            eset.append( RdfEntity.reference(key_commit[0]) )
        #eset= yield self.states.get_states(sortedkeys[RdfBase.ENTITY])
        
        ws=WorkSpace.load(state,aset,eset,sset,bset)
        
        defer.returnValue(ws)
        
        
    @defer.inlineCallbacks        
    def checkout(self,key,commit=None):
        # calls to repos...
        # pass result to WorkSpace.load()
        # service will pass the whole workspace for now
        state= yield self.states.get_key(key,commit)
        
        ws = yield self.checkout_state(state)
        
        defer.returnValue(ws)
        
    @defer.inlineCallbacks    
    def commit(self,workspace):
        # Commit a workspace to the repository
        
        assert isinstance(workspace, WorkSpace)
        # Get the difference between the workspace and its parent
        wdiff = yield self.diff_commit(workspace)
        
        if wdiff.len_associations() == 0:
            logging.info('Nothing to commit')
        else:
            # Commit the new stuff in wdiff
            blobs = workspace.get_blobs()
            yield self.blobs.put_blobs(blobs)
            
            associations = workspace.get_associations()
            yield self.associations.put_associations(associations)
            
            references=workspace.get_references()
            for key in references:
                yield self.a_refs.add_references(key,references[key])
            
            # Don't put states and entities - they already exist!
            
            # Get the current state from the workspace (list of association keys)
            alist = workspace.get_association_list()
            key = workspace.key
            commitRefs = workspace.commitRefs
            
            if commitRefs:
                update=RdfState.load(key,set(alist),commitRefs)
            else:
                update=RdfEntity.load(key,set(alist))
                if key == None:
                    # It was generated in RdfEntity.load because this is a new thing
                    key = update.key
                    
            
            # Returns list of key/commit tuples (length arg to put_states)    
            key_commit= yield self.states.put_states(update)
            assert key == key_commit[0][0]
            commit=key_commit[0][1]
            
            # Add a reference from the commit to each association
            for a in alist:
                yield self.e_refs.add_references(a,[(key,commit)])
                
            wdiff.len_associations()
            workspace.commitRefs=[commit]
            workspace.modified=False
            logging.info('Committed to Key# ' + key)
            logging.info('Commit Ref# ' + str(commit))
            logging.info('Commited Associations:'+ str(wdiff.len_associations()))
            logging.info('Commited Blobs:' + str(wdiff.len_blobs()))

        
    @defer.inlineCallbacks     
    def diff_commit(self,workspace,commit=[]):
        
        assert isinstance(workspace, WorkSpace)
        state=None
        # Trust the modified flag in the workspace
        if workspace.modified or commit:
            # Get the Key
            key = workspace.key

            # if commit not passed, get from the workspace
            if not commit:   
                commit=workspace.commitRefs

            # Get the state of key/commit
            state= yield self.states.get_key(key,commit)
            if not state:
                logging.info('RdfStore:diff - key/commit does not yet exist!')
                # 'everything' is different, return the workspace!
                ws=workspace
        else:
            # there is no difference - return an empty workspace
            ws=WorkSpace.create([])
         
        # Force the comparison!   
        key = workspace.key
        if not commit:   
                commit=workspace.commitRefs
        state= yield self.states.get_key(key,commit)
            
        # IF state, compare the associations and diff the workspace
        if state:
            ws_state=RdfState.load(workspace.key,workspace.get_association_list(),workspace.commitRefs)
            
            if state != ws_state:
                # Must get the difference between the association lists
                ws=workspace.copy()
                
                associations = workspace.get_associations()
                for a in associations:
                    # a is an association in the current workspace, check if it is in the state
                    if a.key in state.object:
                        
                        print a
                        
                        ws.remove_association(a)

                # Could test for differences between the Blobs, but probably not worth it!

            else:
                # there is no difference - return an empty workspace
                ws=WorkSpace.create([])
            
        defer.returnValue(ws)        
    
#    @defer.inlineCallbacks 
    def merge(self,state1, state2):
        pass
        
        
            
    # To be implemented later! Make it distributed so services can work locally!
    def push(self,key,**kwargs):
        pass
    
    def pull(self,key,**kwargs):
        pass
    
    
    def walk(self,rdfbase,association_match):
        
        ws = WorkSpace()
        return ws
        
        
    def garbage_collection(self):
        pass
    
            
        
        
        
        
        
        
        
        
        
        
            
            