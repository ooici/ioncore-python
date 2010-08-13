#!/usr/bin/env python


"""
@file ion/services/dm/preservation/preservation_service.py
@author David Stuebe
@brief service controling preservation of OOI Data
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class PreservationService(BaseService):
    """Preservation Service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='preservation_service', version='0.1.0', dependencies=[])

    def op_create_archive(self, content, headers, msg):
        """Service operation: define a new archive object
        """
        #Content is a topic + metadata
        # Pick a file name
        # set default policy
        # touch the file
        # Allocate storage?
        # Register new Archive
        
    def op_activate_persister(self, content, headers, msg):
        """Service operation: create process to archive a data stream
        """
        # Spawn persister to topic/file name

    def op_deactivate_persister(self, content, headers, msg):
        """Service operation: kill data stream archive process
        """
        # kill persister - Not before LCA
        
    def op_archive_data(self, content, headers, msg):
        """Service operation: archive an single dataset
        """
        # Spawn persister to topic/file name

    def op_set_cache_policy(self, content, headers, msg):
        """Service operation: set the cache policy for an archive
        """

    def op_set_backup_policy(self, content, headers, msg):
        """Service operation: set backup policy for an archive
        """

    def op_set_long_term_policy(self, content, headers, msg):
        """Service operation: set the long term policy for an archive
        """

# Spawn of the process using the module name
factory = ProtocolFactory(PreservationService)