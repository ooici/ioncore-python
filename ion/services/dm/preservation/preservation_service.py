#!/usr/bin/env python


"""
@file ion/services/dm/preservation/preservation_service.py
@author David Stuebe
@brief service controling preservation of OOI Data
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.services.dm.preservation import preservation_registry

from ion.resources import dm_resource_descriptions

class PreservationService(ServiceProcess):
    """Preservation Service interface
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='preservation_service', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        self.reg = yield preservation_registry.PreservationRegistryClient(proc=self)

    @defer.inlineCallbacks
    def op_create_archive(self, content, headers, msg):
        """Service operation: define a new archive object
        """
        #arc = dm_resource_descriptions.ArchiveResource.create_new_resource()

        # Set stuff in arc...
        #Content is a topic + metadata
        # Pick a file name
        # set default policy
        # touch the file
        # Allocate storage?
        # Register new Archive

        arc = yield self.reg.define_archive(arc)

    @defer.inlineCallbacks
    def op_activate_archive_persister(self, content, headers, msg):
        """Service operation: create process to archive a data stream
        """
        # Spawn persister to topic/file name
        persister={'name':'persister 1', # Give it a new name?
                 'module':'ion.services.dm.transformation.persister',
                 'procclass':'Persister',
                 'spawnargs':{'attach':[topic.queue.name],
                              'process parameters':{'fname':arc.name}}}

        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)

    def op_deactivate_archive_persister(self, content, headers, msg):
        """Service operation: kill data stream archive process
        """
        # kill persister - Not before LCA

    def op_archive_data(self, content, headers, msg):
        """Service operation: archive an single dataset
        """
        # Spawn persister to topic/file name

    def op_set_archive_cache_policy(self, content, headers, msg):
        """Service operation: set the cache policy for an archive
        """

    def op_set_archive_backup_policy(self, content, headers, msg):
        """Service operation: set backup policy for an archive
        """

    def op_set_archive_long_term_policy(self, content, headers, msg):
        """Service operation: set the long term policy for an archive
        """

# Spawn of the process using the module name
factory = ProcessFactory(PreservationService)


class PreservationClient(ServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'preservation_service'
        ServiceClient.__init__(self, proc, **kwargs)

    def create_archive(self, data_reg_resource):
        '''
        @brief create a new archive
        @param dataresource is a DM Data Resource which is registered
        @return IngestionDataStreamResource object
        '''

    def activate_archive_persister(self,archive):
        '''
        @brief start the persister
        @param archive is a dm archive resource - the topic field must be valid
        '''

    def deactivate_archive_persister(self,archive):
        '''
        @brief stop the persister
        '''

    def archive_data(self, archive, data):
        '''
        @brief RPC interface to store a single block of data in an archive

        '''

    def set_archive_cache_policy(self,archive):
        '''
        @brief set the caching policy for an archive
        '''

    def set_archive_backup_policy(self,archive):
        '''
        @brief set the backup policy for an archive
        '''

    def set_archive_long_term_policy(self,archive):
        '''
        @brief set the long term storage policy for an archive
        '''
