#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/ingest.py
@author Michael Meisinger
@brief service for ingesting information into DM
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.dm.ingestion import ingestion_registry

class IngestionService(BaseService):
    """Ingestion service interface
    @Note Needs work - Should create a subscription to ingest a data source
    What should the service interface look like for this?
    """

    # Declaration of service
    declare = BaseService.service_declare(name='ingestion_service',
                                          version='0.1.0',
                                          dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        self.reg = yield ingestion_registry.IngestionRegistryClient(proc=self)
        
    
    def op_create_ingestion_datastream(self, content, headers, msg):
        """Service operation: declare new named datastream for ingestion
        
        1) create inbound topic
        2) create ingested topic
        3) start preservation of inbound
        4) start preservation of ingested
        5) start ingestion workflow
        
        Register progress and set lcstate along the way
        
        return the ingestiondatastream resource
    
    
    
        """

        
        


class IngestionClient(BaseServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'ingestion_service'
        BaseServiceClient.__init__(self, proc, **kwargs)

    def create_ingestion_datastream(self,name):
        '''
        @Brief create a new ingestion datastream
        @param name a string naming the new ingestion stream
        @return IngestionDataStreamResource object
        '''
        


 
# Spawn of the process using the module name
factory = ProtocolFactory(IngestionService)
