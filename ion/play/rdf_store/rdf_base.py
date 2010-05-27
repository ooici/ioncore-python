#!/usr/bin/env python

"""
@file ion/play/rdf_store/rdf_service.py
@author David Stuebe
@brief  RDF Base class for the objects used in the rdf store.
"""

import logging

# Use the Json serialization and hashing
from ion.data.objstore import ValueRef

from uuid import uuid4

class RdfBase(object):
    
    ENTITY='E'
    STATE='S'
    ASSOCIATION='A'
    BLOB='B'
    
    SUBJECT='s'
    PREDICATE='p'
    OBJECT='o'
    
    def __init__(self,object,type,key=None, commitRefs=[]):
        self.object=object
        self.type=type

        # The Key for the RDF object
        if key:
            self.key=key
        else:
            self.key=ValueRef._secure_value_hash(object)

        # The commit reference for a particular state
        self.commitRefs=commitRefs



    def __hash__(self):
        '''
        @ Note This should not be a hashable object - it can not be used as a dict key
        http://docs.python.org/reference/datamodel.html?highlight=__cmp__#object.__hash__
        '''
        return None
        
    def __eq__(self,other):
        """
        @brief Object Comparison
        @param DataObject Instance
        @retval True or False
        """
        assert isinstance(other, RdfBase)
        a= self.key == other.key
        b= self.type == other.type
        c= self.object == other.object
        d= self.commitRefs == other.commitRefs
        return a and b and c and d
    
    def __ne__(self,other):
        """
        @brief Object Comparison
        @param other DataObject Instance
        @retval Bool 
        """
        return not self.__eq__(other)


class RdfBlob(RdfBase):
    
    def __init__(self):
        pass

    @classmethod
    def create(cls, value):
        inst=cls()
        RdfBase.__init__(inst,value,RdfBase.BLOB)
        return inst
    
    @classmethod
    def load(cls, key, blob):
        inst=cls()
        RdfBase.__init__(inst,blob,RdfBase.BLOB,key=key)
        return inst



class RdfAssociation(RdfBase):
    
    def __init__(self):
        pass

    @classmethod
    def create(cls, subject, predicate, object):
        assert isinstance(subject, RdfBase)
        assert isinstance(predicate, RdfBase)
        assert isinstance(object, RdfBase)
        
        s=(subject.type,subject.key)
        p=(predicate.type,predicate.key)
        o=(object.type,object.key)
        
        a={ RdfBase.SUBJECT:s,
            RdfBase.PREDICATE:p,
            RdfBase.OBJECT:o }

        inst = cls()
        RdfBase.__init__(inst,a,RdfBase.ASSOCIATION)
        return inst


    @classmethod
    def load(cls, key, association):
        inst=cls()
        RdfBase.__init__(inst,association, RdfBase.ASSOCIATION, key=key)
        return inst
        


class RdfESBase(RdfBase):
    '''
    Make a common class for Entity and State to inherit from
    '''

    def __init__(self):
        pass

    def add(self, association):
        assert isinstance(association,RdfAssociation)
        self.object.add(association.key)
        
        
    def remove(self, association):
        assert isinstance(association,RdfAssociation)
        self.object.discard(association.key)

    def __len__(self):
        return len(self.object)

class RdfEntity(RdfESBase):
    '''
    An RdfEntity can only be created - Never returned from the State Service
    The State service only returns States!
    '''
    
    def __init__(self):
        pass    

    @classmethod
    def create(cls,associations, key=None):
        if not key:
            key=str(uuid4())
        s=set()
        if associations:
            if not getattr(associations, '__iter__', False):
                associations = (associations,)
            assert hasattr(associations, '__iter__')
            
            for a in associations:
                assert isinstance(a, RdfAssociation)
                s.add(a.key)
            
        inst=cls()
        RdfBase.__init__(inst,s,RdfBase.ENTITY,key)
        return inst

    @classmethod
    def load(cls, key, entity):
        inst=cls()
        RdfBase.__init__(inst,entity,RdfBase.ENTITY,key=key)
        return inst
    
    @classmethod
    def reference(cls, key):
        inst=cls()
        RdfBase.__init__(inst,None,RdfBase.ENTITY,key=key)
        return inst
    

        
class RdfState(RdfESBase):
    '''
    The state of an RDF Resource returned from the State Service
    '''
    def __init__(self):
        pass


    @classmethod
    def create(cls,key,associations,commitRefs):
        
        if not getattr(commitRefs, '__iter__', False):
            commitRefs = (commitRefs,)
        assert hasattr(commitRefs, '__iter__')
        assert len(commitRefs)>0

        s=set()
        if associations:
            if not getattr(associations, '__iter__', False):
                associations = (associations,)
            assert hasattr(associations, '__iter__')
            for a in associations:
                assert isinstance(a, RdfAssociation)
                s.add(a.key)        

        inst=cls()
        RdfBase.__init__(inst,s,RdfBase.STATE,key=key,commitRefs=commitRefs)
        return inst

    @classmethod
    def load(cls, key, state, commitRefs):
        inst=cls()
        RdfBase.__init__(inst, state, RdfBase.STATE, key=key, commitRefs=commitRefs)
        return inst

    @classmethod
    def reference(cls, key, commitRefs):
        inst=cls()
        RdfBase.__init__(inst, None, RdfBase.STATE, key=key, commitRefs=commitRefs)
        return inst


    
class WorkSpace(object):
    '''
    Things in the RdfWorkSpace sets are like things in your working directory in git.
    '''

    def __init__(self):
        
        self.commitRefs=None
        self.key=None
        
        self.modified=None
        
        self.workspace={
            RdfBase.ASSOCIATION:{},
            RdfBase.BLOB:{},
            RdfBase.ENTITY:{},
            RdfBase.STATE:{}
        }

        self.references={}

    def add_triple(self,triple):
        association = RdfAssociation.create(triple[0],triple[1],triple[2])
        self.add_association(association,triple)
    
    def add_association(self,association,triple):        
        
        assert type(triple) is tuple
        assert len(triple) ==3

        self.workspace[association.type][association.key]=association
        if not self.references.has_key(association.key):
            self.references[association.key]=set()
            
        for item in triple:
            self.workspace[item.type][item.key]=item
            
            # add a reference to the thing!
            if item.key in self.references:
                self.references[item.key].add(association.key)
            else:
                self.references[item.key]=set([association.key])    
    
    def remove_association(self, association):
        
        assert isinstance(association, RdfAssociation)
        
        # Note that the workspace is modified
        self.modified=True
        
        if len(self.references[association.key]) == 0:
            del self.workspace[association.type][association.key]
        
        for item in association.object:
            type_key = association.object[item]
           
            type = type_key[0]
            key  = type_key[1]
            
            self.references[key].discard(association.key)
            
            if len(self.references[key]) == 0:
               del self.workspace[type][key]
            
    @classmethod
    def load(cls,rdf, associations, entityRefs, stateRefs, blobs):
        '''
        @param rdf An RdfState or RdfEntity object retrieved from the store
        @param associations A set of associations retrieved from the store
        @param entityRefs A set of entity references (a Key)
        @param stateRefs A set of state references (a Key and commit Ref)

        '''
        inst=cls()
        
        # Set the key and commitRefs from state or entity
        inst.key=rdf.key
        if rdf.commitRefs:
            inst.commitRefs=rdf.commitRefs
        
        self.modified=False
        
        for association in associations:
            inst.workspace[RdfBase.ASSOCIATION][association.key]=association
            
        for blob in blobs:
            inst.workspace[RdfBase.BLOB][blob.key]=blob
        
        for entityRef in entityRefs:
            inst.workspace[RdfBase.ENTITY][entityRef.key]=entityRef
        
        for stateRef in stateRefs:
            inst.workspace[RdfBase.STATE][stateRef.key]=stateRef
        return inst

    @classmethod
    def create(cls, associations_triples, key=None):
        '''
        @param associations_triples a list of tuples. Each tuple is an assocation and a triple of RdfBase instances
        '''
        inst=cls()
        inst.key=key
        
        for pair in associations_triples:
            association=pair[0]
            triple=pair[1]
            
            inst.add_association(association,triple)
        
        return inst
        
    def len_associations(self):
        return len(self.workspace[RdfBase.ASSOCIATION])

    def len_blobs(self):
        return len(self.workspace[RdfBase.BLOB])

    def len_entities(self):
        return len(self.workspace[RdfBase.ENTITY])

    def len_states(self):
        return len(self.workspace[RdfBase.STATE])

    def print_status(self):
        print '# of Associations in workspace', self.len_associations()
        print '# of Blobs in workspace', self.len_blobs()
        print '# of Entities in workspace', self.len_entities()
        print '# of States in workspace', self.len_states()

    def len_refs(self,item):
        assert isinstance(item,RdfBase)
        size=0
        if item.key in self.references:
            size = len(self.references[item.key])
        return size

    def diff(self, other):
        assert isinstance(other, workspace)
        # @Todo Impliment me!
        return WorkSpace()