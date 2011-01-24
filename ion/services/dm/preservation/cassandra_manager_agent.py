#!/usr/bin/env python


"""
@file ion/services/dm/preservation/cassandra_manager_agent.py
@author David Stuebe
@author Matt Rodriguez
@brief agent controlling preservation of OOI Data
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


class CassandraManagerAgent(ServiceProcess):
    """
    @Brief Cassandra Manager Agent interface
    @TODO change to inherit from agent process
    The agent uses a single manager client connection to cassandra to configure
    cassandra persistent archive and cache resources. 
    
    Resources can be created by the agent
    Resources can be modified by the agent
    Resources can be deleted by the agent
    
    The agent is also used during a boot strap procedure...
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='cassandra_manager_agent', version='0.1.0', dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Bootstrap args is a dictionary containing a host list and credentials
        self.spawn_args['bootstrap_args'] = self.spawn_args.get('bootstrap_args', CONF.getValue('bootstrap_args', default=None))
        
        # Create a Resource Client 
        self.rc = ResourceClient(proc=self)        
        log.info('PreservationService.__init__()')


        
        
    @defer.inlineCallbacks
    def slc_activate(self, *args):
        """
        Activation can be automatic when the process is spawned or triggered by
        a message from the client.
        
        First, default to bootstrapping from the spawn args. Create all resources
        by describing the cluster!
        
        Second, add hooks to over ride the spawn args and take the storage resource
        reference from a message in op_activate - connecting to an already active
        system!
        
        """
        
        
        if self.spawn_args.get('bootstrap_args', None):
            
            # Build a storage resource from the spawn_args dictionary 
            #self.spawn_args.get(...)
            storage_resource = yield self.rc.create_instance(...)
            #Fill in vals here...
            yield self.rc.put_instance(storage_resource)
        else:
            # Load the state of the service from the resource registry
            
            #Pass a storage resource ID?
            # Lets put it in the spawn args for now...
            #self.spawn_args.get(...)
            storage_resource = yield self.rc.get_instance(storage_resource_ref)
        
        
        # The cassandra client manager
        manager = CassandraDataManager(storage_resource)
        
        #@TODO Need update the register method using Dave F's new code!
        yield self.register_life_cycle_object(manager)
        
        self.manager = manager
        
        # Now query the cluster to create persistence resources 
        if self.spawn_args.get('bootstrap_args', None):
            yield self._bootstrap()
            pass
        
        
        
    @defer.inlineCallbacks
    def _bootstrap(self):
        
        for keyspace in cassandracluster:
            cassandra_keyspace = yield self.rc.create_instance(...)
            yield self.manager.describe_keyspace(cassandra_keyspace)            
            yield self.rc.put_instance(cassandra_keyspace)
        
        for cf in keyspace
            cassandra_cf = yield self.rc.create_instance(...)
            # ...
            # ...
            
        
    @defer.inlineCallbacks
    def op_create_persistent_archive(self, persistent_archive, headers, msg):
        """Service operation: define a new archive object
        
        What should the args be?
        What happens with credentials for a new keyspace?
        
        """
        cassandra_keyspace = yield self.rc.create_instance(...)
        yield self.manager.create_persistent_archive
        yield self.rc.put_instance(cassandra_keyspace)

    @defer.inlineCallbacks
    def op_update_persistent_archive(self, persistent_archive, headers, msg):
        """Service operation: define a new archive object
        
        What should the args be?
        * PA_REF
        * Updated values?
        What happens with credentials for a new keyspace?
        
        """
        cassandra_keyspace = yield self.rc.get_instance(pa_ref)
        
        #make the changes in the resource
        #...
        
        # update cassandra
        yield self.manager.update_persistent_archive
        
        # update the resource
        yield self.rc.put_instance(cassandra_keyspace)


    @defer.inlineCallbacks
    def op_delete_persistent_archive(self, persistent_archive, headers, msg):
        """Service operation: define a new archive object
        
        What should the args be?
        What happens with credentials for a new keyspace?
        
        """
        cassandra_keyspace = yield self.rc.create_instance(...)
        yield self.manager.create_persistent_archive
        yield self.rc.put_instance(cassandra_keyspace)
   
   # more of the same for the cache?
   

# Spawn of the process using the module name
factory = ProcessFactory(PreservationService)


class PreservationClient(ServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'preservation_service'
        ServiceClient.__init__(self, proc, **kwargs)

    def create_persistent_archive(self, persistent_archive):
        """
        @brief create a new archive
        @param dataresource is a DM Data Resource which is registered
        @return IngestionDataStreamResource object
        """
        
    def update_persistent_archive(self, persistent_archive):
        pass
    
    def delete_presistent_archive(self, persistent_archive):
        pass
