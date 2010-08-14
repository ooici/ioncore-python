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

from ion.services.dm.preservation import preservation_registry

from ion.resources import dm_resource_descriptions

class PreservationService(BaseService):
    """Preservation Service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='preservation_service', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        self.reg = yield preservation_registry.PreservationRegistryClient(proc=self)
        
    def op_create_archive(self, content, headers, msg):
        """Service operation: define a new archive object
        """
        
        arc = dm_resource_descriptions.ArchiveResource.create_new_resource()
        
        # Set stuff in arc...
        #Content is a topic + metadata
        # Pick a file name
        # set default policy
        # touch the file
        # Allocate storage?
        # Register new Archive
        
        arc = yield self.reg.define_archive(arc)
        
        
        
    def op_activate_persister(self, content, headers, msg):
        """Service operation: create process to archive a data stream
        """
        # Spawn persister to topic/file name
        persister={'name':'persister 1', # Give it a new name?
                 'module':'ion.services.dm.preservation.persister',
                 'procclass':'Persister',
                 'spawnargs':{'attach':[topic.queue.name],
                              'process parameters':{'fname':arc.name}}}
        
        child1 = base_consumer.ConsumerDesc(**pd1)
        
        child1_id = yield self.test_sup.spawn_child(child1)



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