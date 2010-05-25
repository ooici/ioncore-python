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
        
        
        