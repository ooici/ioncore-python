#!/usr/bin/env python


"""
@file ion/services/dm/preservation/inventory_service.py
@author Matt Rodriguez
@brief agent controlling preservation of OOI Data
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.object import object_utils

from ion.core.data.cassandra import CassandraIndexedStore

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

from ion.core.messaging.message_client import MessageClient

from ion.core.data.store import IIndexStore
from zope.interface import implements

from ion.core import ioninit
CONF = ioninit.config(__name__)

# Persistent Technology Resource:
cassandra_cluster_type =  object_utils.create_type_identifier(object_id=2504, version=1)
# Persistent Archive Resource:
cassandra_keyspace_type =  object_utils.create_type_identifier(object_id=2506, version=1)
# Cache Resource:
cassandra_column_family_type =  object_utils.create_type_identifier(object_id=2507, version=1)
#Credential Resource
cassandra_credential_type =  object_utils.create_type_identifier(object_id=2503, version=1)

cassandra_indexed_row_type = object_utils.create_type_identifier(object_id=2511, version=1)
cassandra_rows_type = object_utils.create_type_identifier(object_id=2512, version=1)
resource_response_type = object_utils.create_type_identifier(object_id=12, version=1)
resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)

class CassandraInventoryServiceException(Exception):
    """
    Exceptions that originate in the CassandraManagerService class
    """

class CassandraInventoryService(ServiceProcess):
    """
    @brief CassandraInventoryService
    
    TODO, this class does not catch any exceptions from the business logic class. 
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='cassandra_inventory_service', version='0.1.0', dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        
        self._host = self.spawn_args.get('bootstrap_args',  CONF.getValue('host'))
        self._port = self.spawn_args.get('bootstrap_args', CONF.getValue('port'))
        self._username = self.spawn_args.get('bootstrap_args', CONF.getValue('username'))
        self._password = self.spawn_args.get('bootstrap_args',CONF.getValue('password'))
        self._keyspace = self.spawn_args.get('bootstrap_args',CONF.getValue('keyspace'))
        self._column_family = self.spawn_args.get('bootstrap_args',CONF.getValue('column_family'))
        # Create a Resource Client 
        self.rc = ResourceClient(proc=self)    
        self.mc = MessageClient(proc=self)    
        

        
        
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
        #Hard code the storage resource for now. Eventually pass all this into spawn_args
        if self._host is None:
            raise CassandraInventoryServiceException("The hostname for the Cassandra cluster is not set.")
        
        if self._port is None:
            raise CassandraInventoryServiceException("The port for the Cassandra cluster is not set.")
        
        if self._username is None:
            raise CassandraInventoryServiceException("The username for the credentials to authenticate to the Cassandra cluster is not set.")
        
        if self._password is None:
            raise CassandraInventoryServiceException("The password for the credentials to authenticate to the Cassandra cluster is not set.")
        
        if self._keyspace is None:
            raise CassandraInventoryServiceException("The keyspace for is not set.")
        
        if self._column_family is None:
            raise CassandraInventoryServiceException("The column family is not set.")
    
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        cassandra_cluster = yield self.rc.create_instance(cassandra_cluster_type, ResourceName="Cassandra cluster", ResourceDescription="OOI Cassandra cluster")
        #persistent_technology_repository, cassandra_cluster  = self.wb.init_repository(cassandra_cluster_type)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        cas_host.host = self._host
        cas_host.port = self._port
        
        #TODO Pass these in through the bootstrap 
        #self._keyspace = "TestKeyspace"
        #self._column_family = "TestRDF"
        
        persistent_archive = yield self.rc.create_instance(cassandra_keyspace_type, ResourceName=self._keyspace, ResourceDescription="description of " + self._keyspace)
        persistent_archive.name = self._keyspace
        
        cache = yield self.rc.create_instance(cassandra_column_family_type, ResourceName=self._column_family, ResourceDescription="description of " + self._column_family)
        cache.name = self._column_family
        
        simple_password = yield self.rc.create_instance(cassandra_credential_type, ResourceName="Cassandra credentials", ResourceDescription="OOI Cassandra credentials")
        simple_password.username = self._username
        simple_password.password = self._password
        

        log.info("Creating Cassandra Store")
        self._indexed_store = CassandraIndexedStore(cassandra_cluster, persistent_archive, simple_password, cache)
        yield self.register_life_cycle_object(self._indexed_store)

        log.info("Created Cassandra Store")
        
        
        

    @defer.inlineCallbacks
    def op_query(self, request, headers, msg):
        """
        @Note The goal is to return a dictionary of keys and resourceids.
        @retval return a cassandra_rows type. The key attribute will be set and each row will contain one column 
        with the name value.
        """
        
        cassandra_row = request
        index_attrs = {}
        for attr in cassandra_row.attrs:
            index_attrs[attr.attribute_name] = attr.attribute_value
        results = yield self._indexed_store.query(index_attrs)
        #Now we have to put these back into a response
        response = yield self.mc.create_instance(cassandra_rows_type, MessageName="query response")
        
    
        #The GPB buffer object represents cassandra rows, we could probably get away with just making a dictionary like
        #object, since that's what query returns.
        for key,value in results.items():
            r = response.rows.add()
            r.key = key
            col = r.cols.add()
            col.column_name = "value"
            col.value = value
        
           
        yield self.reply_ok(msg,response)
            
    @defer.inlineCallbacks
    def op_put(self, request, headers, msg):
        """
        @note, puts a row into the Cassandra cluster. 
        @retval does not return anything
        """
        cassandra_row = request
        key = cassandra_row.key
        value = cassandra_row.value
        index_attrs = {}
        for attr in cassandra_row.attrs:
            index_attrs[attr.attribute_name] = attr.attribute_value
        yield self._indexed_store.put(key,value,index_attrs)    
        
        response = yield self.mc.create_instance(resource_response_type, MessageName="put response")
        response.result= 'Put complete'
        yield self.reply_ok(msg, response)
        
        
    @defer.inlineCallbacks
    def op_get(self, request, headers, msg):
        """
        @note Gets a row from the Cassandra cluster
        If the row does not exist then leave the value field in the CassandraIndexedRow empty.
        @request is a CassandraRow message object
        @retval Returns a CassandraRow message in the response   
        """
        cassandra_row = request
        key = cassandra_row.key
        
        value = yield self._indexed_store.get(key)        
        response = yield self.mc.create_instance(cassandra_indexed_row_type, MessageName="get response")

        if value is not None:
            response.value = value
        
        yield self.reply_ok(msg, response)
         
    @defer.inlineCallbacks
    def op_remove(self, request, headers, msg): 
        """
        @note removes a row
        @request is a CassandraRow message object
        @retval does not return anything
        """     
        cassandra_row = request
        key = cassandra_row.key
        yield self._indexed_store.remove(key)
        response = yield self.mc.create_instance(resource_response_type, MessageName="remove response")
        response.result= 'Remove complete'
        yield self.reply_ok(msg, response)
        
     
    @defer.inlineCallbacks
    def op_get_query_attributes(self, request, headers, msg):
        """
        @note gets the names of the columns that are indexed in the column family
        @retval returns the names of the columns in a CassandraRow message
        """      
        column_list = yield self._indexed_store.get_query_attributes()
        response = yield self.mc.create_instance(cassandra_indexed_row_type, MessageName="get_query_attributes response")
        
        for column_name in column_list:
            attr = response.attrs.add()
            attr.attribute_name = column_name
             
        yield self.reply_ok(msg, response)
# Spawn of the process using the module name
factory = ProcessFactory(CassandraInventoryService)


class CassandraInventoryClient(ServiceClient):
    """
    This interface will change, because we have to define the ION resources. We probably want
    convenience methods to query by name, type, etc...
    
    TODO have this implement the Indexstore interface
    """
    implements(IIndexStore)
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'cassandra_inventory_service'
        ServiceClient.__init__(self, proc, **kwargs)
        
        self.mc = MessageClient(proc=proc)    

    
      
    @defer.inlineCallbacks
    def query(self, index_attributes):
        log.info("Called CassandraInventoryService client")
        
        cassandra_row = yield self.mc.create_instance(cassandra_indexed_row_type, MessageName='Creating a create_request')
        
        for attr_key,attr_value in index_attributes.items():
            attr = cassandra_row.attrs.add()
            attr.attribute_name = attr_key
            attr.attribute_value = attr_value
        

        (content, headers, msg) = yield self.rpc_send('query', cassandra_row)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def put(self, key, value, index_attributes={}):
        log.info("Called CassandraInventoryService client")
        
        cassandra_row = yield self.mc.create_instance(cassandra_indexed_row_type, MessageName='Creating a create_request')
        cassandra_row.key = key
        cassandra_row.value = value
        
        for attr_key,attr_value in index_attributes.items():
            attr = cassandra_row.attrs.add()
            attr.attribute_name = attr_key
            attr.attribute_value = attr_value 
        
        (content, headers, msg) = yield self.rpc_send('put', cassandra_row)
        defer.returnValue(content)
    
    @defer.inlineCallbacks
    def get(self, key):
        
        cassandra_row = yield self.mc.create_instance(cassandra_indexed_row_type, MessageName='Creating a create_request')
        cassandra_row.key = key
          
        (content, headers, msg) = yield self.rpc_send('get',cassandra_row)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def remove(self, key):
        cassandra_row = yield self.mc.create_instance(cassandra_indexed_row_type, MessageName='Creating a create_request')
        cassandra_row.key = key
          
        (content, headers, msg) = yield self.rpc_send('remove', cassandra_row)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def get_query_attributes(self):
        """
        This request does not send any argument. The message is used as a dummy argument.
        """
        
        cassandra_row = yield self.mc.create_instance(cassandra_indexed_row_type, MessageName='Creating a create_request')
        
        (content, headers, msg) = yield self.rpc_send('get_query_attributes', cassandra_row)
        defer.returnValue(content)
        
        
           
