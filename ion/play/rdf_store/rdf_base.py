#!/usr/bin/env python

"""
@file ion/play/rdf_store/rdf_service.py
@author David Stuebe
@brief  RDF Store: rdf service
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.core import bootstrap
from ion.services.base_service import BaseService, BaseServiceClient

from ion.data.dataobject import DataObject
from ion.data.store import Store
from ion.data.objstore import ValueObject, ValueRef

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
        
        s={subject.type:subject.key}
        p={predicate.type:predicate.key}
        o={object.type:object.key}
        
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
        



class RdfEntity(RdfBase):
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
        l=list()
        for a in associations:
            assert isinstance(a, RdfAssociation)
            l.append(a.key)        
        inst=cls()
        RdfBase.__init__(inst,l,RdfBase.ENTITY,key)
        return inst

    @classmethod
    def load(cls, key, entity):
        inst=cls()
        RdfBase.__init__(inst,entity,RdfBase.ENTITY,key=key)
        return inst
        
        
class RdfState(RdfBase):
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
        l=list()
        for a in associations:
            assert isinstance(a, RdfAssociation)
            l.append(a.key)        
        inst=cls()
        RdfBase.__init__(inst,l,RdfBase.STATE,key=key,commitRefs=commitRefs)
        return inst

    @classmethod
    def load(cls, key, state, commitRefs):
        inst=cls()
        RdfBase.__init__(inst, state, RdfBase.STATE, key=key, commitRefs=commitRefs)
        return inst





#class RdfObject(object):
#
#    def __init__(self,backend=backend,backendargs=backendargs):
#        
#        
#        self.backend=backend
#        self.backendargs=backendargs
#        assert issubclass(self.backend, IStore)
#        assert type(self.backargs) is dict
#        
#        
#        self.rdf=rdf=None
#        
#        self.blobs={}  # ID:Blob
#        self.associations={} # ID:Association
#        self.entity={} # ID:Entity
#
#    def add_association(self,subject,predicate,object):
#        
#        s={subject.TYPE:subject.key}
#        p={predicate.TYPE:predicate.key}
#        o={object.TYPE:object.key}
#            
            
            
            
            
            
            
            
            
            
            
            
            