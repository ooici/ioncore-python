#!/usr/bin/env python

"""
@file ion/services/dm/ingest.py
@author Michael Meisinger
@brief service for ingesting information into DM
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class IngestService(BaseService):
    """Ingestion service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='ingest', version='0.1.0', dependencies=[])
 
    def op_ingest(self, content, headers, msg):
        """Service operation: TBD
        """
        datasetId = None
        dataMessage = None
        dataDesc = None
        
# Spawn of the process using the module name
factory = ProtocolFactory(IngestService)
