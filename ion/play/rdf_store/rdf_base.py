#!/usr/bin/env python

"""
@file ion/play/rdf_store/rdf_service.py
@author David Stuebe
@brief  RDF Store: rdf service
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
        


class RdfWorkSpace(RdfBase):
    '''
    Add some storage and methods for higher level rdf objects.
    Things in the RdfWorkSpace sets are like things in your working directory in git.
    '''

    def __init__(self):
        
        self.workspace={
            self.ASSOCIATION:{},
            self.BLOB:{},
            self.ENTITY:{},
            self.STATE:{}
        }

        self.references={}

    def add(self,triple):
        
        assert type(triple) is tuple
        assert len(triple) ==3
        
        rdfa = RdfAssociation.create(triple[0],triple[1],triple[2])
        self.object.add(rdfa.key)
        
        for item in triple:
            
            self.workspace[item.type][item.key]=item
            
            # add a reference to the thing!
            if item.key in self.references[item.key]:
                self.references[item.key].add(rdfa.key)
            else:
                self.references[item.key]=set([rdfa.key])
        
        
    def discard(self, association):
        assert isinstance(association,RdfAssociation)
        self.object.discard(association.key)
        
        for item in association.object:
            type_key = association.object[item]
           
            type = type_key[0]
            key  = type_key[1]
            
            self.references[key].discard(association.key)
            
            if len(self.references[key]) == 0:
               del self.workspace[type][key]


class RdfEntity(RdfWorkSpace):
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
        
        if not getattr(associations, '__iter__', False):
            associations = (associations,)
        assert hasattr(associations, '__iter__')
        s=set()
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
    

        
class RdfState(RdfWorkSpace):
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
        
        if not getattr(associations, '__iter__', False):
            associations = (associations,)
        assert hasattr(associations, '__iter__')
        s=set()
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


    
    
            
            
            
            
            
            
            
            
            