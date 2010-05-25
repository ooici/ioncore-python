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

from ion.play.rdf_store.association_service import AssociationServiceClient
from ion.play.rdf_store.blob_service import BlobServiceClient
from ion.play.rdf_store.reference_service import ReferenceServiceClient

from uuid import uuid4

class RdfBase(object):
    
    ENTITY='E'
    STATE='S'
    ASSOCIATION='A'
    BLOB='B'
    
    SUBJECT='s'
    PREDICATE='p'
    OBJECT='o'
    
    def __init__(self,object,type,key=None):
        self.object=object
        self.type=type
        if key:
            self.key=key
        else:
            self.key=ValueRef._secure_value_hash(object)


class RdfBlob(RdfBase):
    
    def __init__(self, value):
        RdfBase.__init__(value,RdfBase.BLOB)

class RdfAssociation(RdfBase):
    
    def __init__(self,subject,predicate,object):
        assert issubclass(subject, RdfBase)
        assert issubclass(predicate, RdfBase)
        assert issubclass(object, RdfBase)
        
        s={subject.TYPE:subject.key}
        p={predicate.TYPE:predicate.key}
        o={object.TYPE:object.key}
        
        a={ RdfBase.SUBJECT:s,
            RdfBase.PREDICATE:p,
            RdfBase.OBJECT:o }
        
        RdfBase.__init__(a,RdfBase.ASSOCIATION)



    @classmethod
    def create(cls, association):
        inst = cls()
        inst.set_state(associations)
        return inst
        



class RdfEntity(RdfBase):
    '''
    An RdfEntity can only be created - Never returned from the State Service
    The State service only returns States!
    '''
    
    def __init__(self, key=None):
        if not key:
            key=str(uuid4())
        RdfBase.__init__(None,RdfBase.Entity,key)

    def set_state(self, associations):
        # @TODO prefer Set over List, but Set is not jsonable!

        l=list()
        for a in associations:
            assert isinstance(a, RdfAssociation)
            l.append(a.hash)
        self.object=l
        
    @classmethod
    def create(cls, associations):
        inst = cls()
        inst.set_state(associations)
        return inst
        
        
        
class RdfState(RdfBase):
    '''
    The state of an RDF Resource returned from the State Service
    '''
    def __init__(self, CommitRef):
        RdfBase.__init__(None,RdfBase.State,CommitRef)

    def set_state(self, associations):
        l=list()
        for a in associations:
            assert isinstance(a, RdfAssociation)
            l.append(a.hash)
        self.object=l



class RdfObject(object):

    def __init__(self,rdf):
        
        assert isinstance(rdf,(RdfState,RdfEntity))
        self.rdf=rdf
        
        self.blobs={}  # ID:Blob
        self.associations={} # ID:Association
        self.entity={} # ID:Entity

    def add_association(self,subject,predicate,object):
        
        s={subject.TYPE:subject.key}
        p={predicate.TYPE:predicate.key}
        o={object.TYPE:object.key}
            
        
            
            
            
        



class RdfStore(object):
    
    def __init__(self):
        self.asc=None     # Association Service Client
        self.bsc=None     # Blob Service Client
        self.asc_ref=None # Association Reference Service Client
        self.ssc=None     # State Service Client
        self.ssc_ref=None # State Reference Service Client
        print 'Initialized RdfStore'
    
    @defer.inlineCallbacks
    def init(self):
        services = [{'name':'blob1','module':'ion.play.rdf_store.blob_service','class':'BlobService'},]
        sup = yield bootstrap.spawn_processes(services)
        print 'blob sup', sup
        self.bsc = BlobServiceClient(proc=sup)
        print 'blob service client', self.bsc

        services = [{'name':'association1','module':'ion.play.rdf_store.association_service','class':'AssociationService'},]
        sup = yield bootstrap.spawn_processes(services)
        self.bsc = AssociationServiceClient(proc=sup)

        services = [{'name':'reference1','module':'ion.play.rdf_store.reference_service','class':'ReferenceService'},]
        sup = yield bootstrap.spawn_processes(services)
        self.ssc_ref = ReferenceServiceClient(proc=sup)

        services = [{'name':'reference2','module':'ion.play.rdf_store.reference_service','class':'ReferenceService'},]
        sup = yield bootstrap.spawn_processes(services)
        self.asc_ref = ReferenceServiceClient(proc=sup)
        
        
        