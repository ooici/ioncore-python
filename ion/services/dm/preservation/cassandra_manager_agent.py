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
from ion.core.object import workbench

from ion.core import ioninit
CONF = ioninit.config(__name__)

host_list_type = object_utils.create_type_identifier(object_id=2501, version=1)
simple_password_type = object_utils.create_type_identifier(object_id=2502, version=1)
cassandra_credential_type =  object_utils.create_type_identifier(object_id=2503, version=1)
column_def_type =  object_utils.create_type_identifier(object_id=2508, version=1)

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
        
        self._host = CONF.getValue('host')
        self._port = CONF.getValue('port')
        self._username = CONF.getValue('username')
        self._password = CONF.getValue('password')
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
        
        self.wb = workbench.WorkBench("I need this make ION resources")
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        persistent_technology_repository, cassandra_cluster  = self.wb.init_repository(cassandra_cluster_type)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        cas_host.host = self._host
        cas_host.port = self._port
        
        ### Create a Credentials resource - for cassandra a SimplePassword object
        cache_repository, simple_password  = self.wb.init_repository(simple_password_type)
        simple_password.username = self._username
        simple_password.password = self._password
        
        storage_resource = CassandraStorageResource(cassandra_cluster, credentials=simple_password)
        manager = CassandraDataManager(storage_resource)
        
        #@TODO Need update the register method using Dave F's new code!
        #yield self.register_life_cycle_object(manager)
        
        self.manager = manager
        
        # Now query the cluster to create persistence resources 
        if self.spawn_args.get('bootstrap_args', None):
            yield self._bootstrap()
            
        
        
        
    @defer.inlineCallbacks
    def _bootstrap(self):
        yield 1
        #Implement this later
        """
        for keyspace in cassandracluster:
            cassandra_keyspace = yield self.rc.create_instance(...)
            yield self.manager.describe_keyspace(cassandra_keyspace)            
            yield self.rc.put_instance(cassandra_keyspace)
        
        for cf in keyspace
            cassandra_cf = yield self.rc.create_instance(...)
            # ...
            # ...
            
        """
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
        #yield self.manager.create_persistent_archive(pa)
        
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
        
        persistent_archive_resource = yield self.rc.get_instance(request.resource_reference)
        #Check to see if the resource has changed
        log.info("put instance")
        yield self.rc.put_instance(persistent_archive_resource)
        #Do the business logic
        #yield self.manager.update_persistent_archive(persistent_archive)
        
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
        
        
        persistent_archive_resource = yield self.rc.get_instance(request.resource_reference)
        #We want to delete this resource from the registry here
        
        #yield self.manager.delete_persistent_archive(persistent_archive)
        response = yield self.mc.create_instance(resource_response_type, name="update_persistent_archive_response")
        response.resource_reference = self.rc.reference_instance(persistent_archive_resource)
        
        response.configuration = persistent_archive_resource.ResourceObject
        response.result = 'Deleted'
        yield self.reply_ok(msg, response)
   
   

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
