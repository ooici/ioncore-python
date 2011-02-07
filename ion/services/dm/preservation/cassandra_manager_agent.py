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

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

from ion.core.messaging.message_client import MessageClient

from ion.core import ioninit
CONF = ioninit.config(__name__)

host_list_type = object_utils.create_type_identifier(object_id=2501, version=1)
simple_password_type = object_utils.create_type_identifier(object_id=2502, version=1)
cassandra_credential_type =  object_utils.create_type_identifier(object_id=2503, version=1)
columndef_type =  object_utils.create_type_identifier(object_id=2508, version=1)

# Persistent Technology Resource:
cassandra_cluster_type =  object_utils.create_type_identifier(object_id=2504, version=1)

# Persistent Archive Resource:
cassandra_keyspace_type =  object_utils.create_type_identifier(object_id=2506, version=1)

# Cache Resource:
column_family_type =  object_utils.create_type_identifier(object_id=2507, version=1)

resource_response_type = object_utils.create_type_identifier(object_id=12, version=1)

class CassandraManagerServiceException(Exception):
    """
    Exceptions that originate in the CassandraManagerService class
    """

class CassandraManagerService(ServiceProcess):
    """
    @brief Cassandra Manager Agent interface
    @TODO change to inherit from agent process
    The agent uses a single manager client connection to cassandra to configure
    cassandra persistent archive and cache resources. 
    
    Resources can be created by the agent
    Resources can be modified by the agent
    Resources can be deleted by the agent
    
    The agent is also used during a boot strap procedure...
    
    @TODO This class just does the messaging, it does not actually do the business logic. I want
    to make sure the messaging works using the new resource/messaging framework.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='cassandra_manager_agent', version='0.1.0', dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Bootstrap args is a dictionary containing a host list and credentials
        #self.spawn_args['bootstrap_args'] = self.spawn_args.get('bootstrap_args', CONF.getValue('bootstrap_args', default=None))
        
        self._host = self.spawn_args.get('bootstrap_args',  CONF.getValue('host'))
        self._port = self.spawn_args.get('bootstrap_args', CONF.getValue('port'))
        self._username = self.spawn_args.get('bootstrap_args', CONF.getValue('username'))
        self._password = self.spawn_args.get('bootstrap_args',CONF.getValue('password'))
        # Create a Resource Client 
        self.rc = ResourceClient(proc=self)    
        self.mc = MessageClient(proc=self)    
        log.info('CassandraManagerAgent.__init__()')


        
        
    @defer.inlineCallbacks
    def slc_activate(self, *args):
        """
        Activation can be automatic when the process is spawned or triggered by
        a message from the client.
        
        First, default to bootstrapping from the spawn args. Create all resources
        by describing the cluster!
        
        Second, add hooks to override the spawn args and take the storage resource
        reference from a message in op_activate - connecting to an already active
        system!
        
        """
        
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
        """
        #Hard code the storage resource for now. Eventually pass all this into spawn_args
        if self._host is None:
            raise CassandraManagerServiceException("The hostname for the Cassandra cluster is not set.")
        
        if self._port is None:
            raise CassandraManagerServiceException("The port for the Cassandra cluster is not set.")
        
        if self._username is None:
            raise CassandraManagerServiceException("The username for the credentials to authenticate to the Cassandra cluster is not set.")
        
        if self._password is None:
            raise CassandraManagerServiceException("The password for the credentials to authenticate to the Cassandra cluster is not set.")
        
        #self.wb = workbench.WorkBench("I need this create resources")
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        cassandra_cluster = yield self.rc.create_instance(cassandra_cluster_type, name="Cassandra cluster", description="OOI Cassandra cluster")
        #persistent_technology_repository, cassandra_cluster  = self.wb.init_repository(cassandra_cluster_type)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        cas_host.host = self._host
        cas_host.port = self._port
        
        ### Create a Credentials resource - for cassandra a SimplePassword object
        #cache_repository, simple_password  = self.wb.init_repository(simple_password_type)
        simple_password = yield self.rc.create_instance(simple_password_type, name="Cassandra credentials", description="OOI Cassandra credentials")
        simple_password.username = self._username
        simple_password.password = self._password
        
        storage_resource = CassandraStorageResource(cassandra_cluster, credentials=simple_password)
        manager = CassandraDataManager(storage_resource)
        manager.initialize() 
        manager.activate()
        #@TODO Need update the register method using Dave F's new code!
        #yield self.register_life_cycle_object(manager)
        
        self.manager = manager
        yield self._bootstrap()
        # Now query the cluster to create persistence resources 
        #if self.spawn_args.get('bootstrap_args', None):
        #    yield self._bootstrap()
            
   
    @defer.inlineCallbacks
    def slc_terminate(self, *args):
        log.info("In CassandraManagerService.slc_terminate")
        yield self.manager.deactivate()   
        
        
    @defer.inlineCallbacks
    def _bootstrap(self):
        """
        Query the cassandra cluster creating resources for each keyspace 
        and column family.
        
        @note the persistent archive resources do not have reference to the cache resources.
        """
        log.info("In CassandraManagerService._bootstrap method")
        desc = yield self.manager._describe_keyspaces()
        log.info(str(desc))
        for ks in desc:
            persistent_archive = yield self.rc.create_instance(cassandra_keyspace_type, name=ks.name, description="description of " + ks.name)
            persistent_archive.name = ks.name
            for cf in ks.cf_defs:
                cache = yield self.rc.create_instance(column_family_type, name=cf.name, description="description of " + cf.name)
                cache.name = cf.name
                column_index = 0
                for col in cf.column_metadata:
                    column = yield self.rc.create_instance(columndef_type,name=col.name, description="description of " + col.name)
                    column.column_name = col.name
                    column.validation_class = col.validation_class
                    if col.index_name is not None:
                        column.index_name = col.index_name
                    if col.index_type is not None:
                        column.index_type = col.index_type
                    cache.column_metadata.add()
                    cache.column_metadata[column_index] = self.rc.reference_instance(column)
                    column_index = column_index + 1

    @defer.inlineCallbacks
    def op_create_persistent_archive(self, request, headers, msg):
        """
        Service operation: define a new archive object
        """
        log.info("Called CassandraManagerService.op_create_persistent_archive")
        persistent_archive = request.configuration
        log.info("request.configuration %s" % (request.configuration,))
        persistent_archive_resource = yield self.rc.create_instance(cassandra_keyspace_type, name=persistent_archive.name,
                                  description="A description")
        
        
        log.info("Created resource")
        yield self.rc.put_instance(persistent_archive_resource, "A commit message")
        log.info("Put resource into datastore")
        yield self.manager.create_persistent_archive(persistent_archive)
        
        response = yield self.mc.create_instance(resource_response_type, name="create_persistent_archive_response")
        response.resource_reference = self.rc.reference_instance(persistent_archive_resource)
        
        # pass the current configuration
        response.configuration = persistent_archive_resource.ResourceObject
        
        # pass the result of the create operation...
        response.result = 'Created'
                
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)
        

    @defer.inlineCallbacks
    def op_update_persistent_archive(self, request, headers, msg):
        """
        Service operation: update the persistent_archive
        
        """
        log.info("In op_update_persistent_archive")
        persistent_archive = request.configuration
        persistent_archive_resource = yield self.rc.get_instance(request.resource_reference)
        #Check to see if the resource has changed
        log.info("put instance")
        yield self.rc.put_instance(persistent_archive_resource)
        #Do the business logic
        yield self.manager.update_persistent_archive(persistent_archive)
        
        log.info("created response")
        response = yield self.mc.create_instance(resource_response_type, name="update_persistent_archive_response")
        response.resource_reference = self.rc.reference_instance(persistent_archive_resource)
        
        response.configuration = persistent_archive_resource.ResourceObject
        response.result = 'Updated'
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_delete_persistent_archive(self, request, headers, msg):
        """
        Service operation: delete the archive
        """
        
        log.info("request: " + str(request))
        persistent_archive = request.configuration
        persistent_archive_resource = yield self.rc.get_instance(request.resource_reference)
        #We want to delete this resource from the registry here
        
        yield self.manager.remove_persistent_archive(persistent_archive)
        response = yield self.mc.create_instance(resource_response_type, name="delete_persistent_archive_response")
        response.resource_reference = self.rc.reference_instance(persistent_archive_resource)
        
        response.configuration = persistent_archive_resource.ResourceObject
        response.result = 'Deleted'
        yield self.reply_ok(msg, response)
   
    @defer.inlineCallbacks
    def op_create_cache(self, request, headers, msg):
        """
        Service operation: define a new persistent archive object
        
        These methods need a way to pass the persistent_archive and cache objects together in the
        request. 
        """
        yield 1
    
    @defer.inlineCallbacks
    def op_update_cache(self, request, headers, msg):
        """
        Service operation: update a cache object
        
        """
        yield 1
         
    @defer.inlineCallbacks
    def op_delete_cache(self, request, headers, msg):       
        """
        Service operation: remove the cache object
        """
        yield 1
        
# Spawn of the process using the module name
factory = ProcessFactory(CassandraManagerService)


class CassandraManagerClient(ServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'cassandra_manager_agent'
        ServiceClient.__init__(self, proc, **kwargs)
    
    @defer.inlineCallbacks
    def create_persistent_archive(self, persistent_archive):
        """
        @brief create a new archive
        @param persistent_archive is an ion resource which defines the properties of a Keyspace
        """
        log.info("Called CassandraManagerClient.create_persistent_archive")
        (content, headers, msg) = yield self.rpc_send('create_persistent_archive', persistent_archive)
        defer.returnValue(content)
      
    @defer.inlineCallbacks    
    def update_persistent_archive(self, persistent_archive):
        """
        @brief update an archive
        @param persistent_archive is an ion resource which defines the properties of a Keyspace
        """
        (content, headers, msg) = yield self.rpc_send('update_persistent_archive', persistent_archive)
        defer.returnValue(content)
    
    @defer.inlineCallbacks
    def delete_persistent_archive(self, persistent_archive):
        """
        @brief remove an archive
        @param persistent_archive is an ion resource which defines the properties of a Keyspace
        """
        (content, headers, msg) = yield self.rpc_send('delete_persistent_archive', persistent_archive)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def create_cache(self, persistent_archive):
        """
        @brief create a new archive
        @param persistent_archive is an ion resource which defines the properties of a Keyspace
        """
        log.info("Called CassandraManagerClient.create_cache")
        (content, headers, msg) = yield self.rpc_send('create_cache', persistent_archive)
        defer.returnValue(content)
      
    @defer.inlineCallbacks    
    def update_cache(self, persistent_archive):
        """
        @brief update an archive
        @param persistent_archive is an ion resource which defines the properties of a Keyspace
        """
        (content, headers, msg) = yield self.rpc_send('update_cache', persistent_archive)
        defer.returnValue(content)
    
    @defer.inlineCallbacks
    def delete_cache(self, persistent_archive):
        """
        @brief remove an archive
        @param persistent_archive is an ion resource which defines the properties of a Keyspace
        """
        (content, headers, msg) = yield self.rpc_send('delete_cache', persistent_archive)
        defer.returnValue(content)    
