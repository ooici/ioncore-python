#!/usr/bin/env python


"""
@file ion/services/dm/preservation/preservation_service.py
@author David Stuebe
@author Matt Rodriguez
@brief service controlling preservation of OOI Data
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.object import object_utils
from ion.core.data.cassandra import CassandraDataManager, CassandraStorageResource



host_list_type = object_utils.create_type_identifier(object_id=2501, version=1)
simple_password_type = object_utils.create_type_identifier(object_id=2502, version=1)
cassandra_credential_type =  object_utils.create_type_identifier(object_id=2503, version=1)
column_def_type =  object_utils.create_type_identifier(object_id=2508, version=1)

# Persistent Technology Resource:
cassandra_cluster_type =  object_utils.create_type_identifier(object_id=2504, version=1)

# Persistent Archive Resource:
cassandra_keySpace_type =  object_utils.create_type_identifier(object_id=2506, version=1)

# Cache Resource:
column_family_type =  object_utils.create_type_identifier(object_id=2507, version=1)


class PreservationService(ServiceProcess):
    """Preservation Service interface
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='preservation_service', version='0.1.0', dependencies=[])

     def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Bootstrap args is a dictionary containing a host list and credentials
        self.spawn_args['bootstrap_args'] = self.spawn_args.get('bootstrap_args', CONF.getValue('bootstrap_args', default=None))
        
        # Create a Resource Client 
        self.rc = ResourceClient(proc=self)
        
        log.info('PreservationService.__init__()')


    @defer.inlineCallbacks
    def slc_init(self):
        
        if self.spawn_args.get('bootstrap_args', None):
            
            # Boot strap the configuration by querying the cassandra cluster
            self.manager = yield self._bootstrap()
            
        else:
            # Get the state of the service from the state service
            self.manager = yield self._get_state()
        
        
        # The cassandra client manager
        self.manager = CassandraDataManager(storage_resource)
        
    @defer.inlineCallbacks
    def _get_state(self):
        
        # Get the state from the state repository to get the current state of
        #the cluster and build a storage_resource
        
        # The cassandra client manager
        manager = CassandraDataManager(storage_resource)
        
        manager.initialize() 
        manager.activate()
        
        return defer.succeed(manager)
        
        
    @defer.inlineCallbacks
    def _bootstrap(self):
        # The cassandra client manager
        manager = CassandraDataManager(storage_resource)
        
        manager.initialize() 
        manager.activate()
        
        return defer.succeed(manager)
        
        
        
    @defer.inlineCallbacks
    def slc_activate(self):
        yield self.register_life_cycle_object(self.manager)
        
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
