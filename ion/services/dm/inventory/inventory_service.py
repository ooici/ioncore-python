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

from ion.core.data.store import IIndexStore, IndexStore
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
    
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        cassandra_cluster = yield self.rc.create_instance(cassandra_cluster_type, name="Cassandra cluster", description="OOI Cassandra cluster")
        #persistent_technology_repository, cassandra_cluster  = self.wb.init_repository(cassandra_cluster_type)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        cas_host.host = self._host
        cas_host.port = self._port
        
        #TODO Pass these in through the bootstrap 
        
        keyspace = "TestKeyspace"
        column_family = "TestCF"
        
        persistent_archive = yield self.rc.create_instance(cassandra_keyspace_type, name=keyspace, description="description of " + keyspace)
        
        cache = yield self.rc.create_instance(cassandra_column_family_type, name=column_family, description="description of " + column_family)
        
        
        simple_password = yield self.rc.create_instance(cassandra_credential_type, name="Cassandra credentials", description="OOI Cassandra credentials")
        simple_password.username = self._username
        simple_password.password = self._password
        
        ### Create a Credentials resource - for cassandra a SimplePassword object
        #cache_repository, simple_password  = self.wb.init_repository(simple_password_type)
        log.info("Creating Cassandra Store")
        self._indexed_store = CassandraIndexedStore(cassandra_cluster,persistent_archive,  simple_password,cache)
        self._indexed_store.activate()
        self._indexed_store.initialize()
        #self._indexed_store = IndexStore()
        log.info("Created Cassandra Store")
        
        
        

    @defer.inlineCallbacks
    def op_query(self, request, headers, msg):
        """
        @Note The goal is to return a dictionary of keys and resourceids.
        @retval return a cassandra_rows type. The key attribute will be set and each row will contain one column 
        with the name value.
        """
        
        cassandra_row = request.configuration
        index_attrs = {}
        for attr in cassandra_row.attrs:
            index_attrs[attr.attribute_name] = attr.attribute_value
        results = yield self._indexed_store.query(index_attrs)
        #Now we have to put these back into a response
        response = yield self.mc.create_instance(resource_response_type, name="query response")
        
        rows_resource = yield self.rc.create_instance(cassandra_rows_type, name="rows back",
                                  description="A description")
        #The GPB buffer object represents cassandra rows, we could probably get away with just making a dictionary like
        #object, since that's what query returns.
        for row in results.items():
            r = rows_resource.row.add()
            r.key = row[0]
            col = r.cols.add()
            col.name = "value"
            col.value = row[1]
        
        response.configuration = rows_resource.ResourceObject  
        response.result = 'Query complete'
           
        yield self.reply_ok(msg, response)
            
    @defer.inlineCallbacks
    def op_put(self, request, headers, msg):
        cassandra_row = request.configuration
        key = cassandra_row.key
        value = cassandra_row.value
        index_attrs = {}
        for attr in cassandra_row.attrs:
            index_attrs[attr.attribute_name] = attr.attribute_value
        yield self._indexed_store.put(key,value,index_attrs)    
        
        response = yield self.mc.create_instance(resource_response_type, name="put response")
        response.result= 'Put complete'
        yield self.reply_ok(msg, response)
        
    @defer.inlineCallbacks
    def op_get(self, request, headers, msg):   
        cassandra_row = request.configuration
        key = cassandra_row.key
        
        value = yield self._indexed_store.get(key)
        
        response = yield self.mc.create_instance(resource_response_type, name="get response")
        response.result= 'Get complete'
        row_resource = yield self.rc.create_instance(cassandra_indexed_row_type, name="value",
                                  description="A description")
        row_resource.value = value
        response.configuration = row_resource.ResourceObject
        yield self.reply_ok(msg, response)
         
    @defer.inlineCallbacks
    def op_remove(self, request, headers, msg):      
        cassandra_row = request.configuration
        key = cassandra_row.key
        yield self._indexed_store.remove(key)
        response = yield self.mc.create_instance(resource_response_type, name="remove response")
        response.result= 'Remove complete'
        yield self.reply_ok(msg, response)
        
     
    @defer.inlineCallbacks
    def op_get_query_attributes(self, request, headers, msg):      
        attr_dict = yield self._indexed_store.get_query_attributes()
        response = yield self.mc.create_instance(resource_response_type, name="get_query_attributes response")
        response.result= 'Get complete'
        row_resource = yield self.rc.create_instance(cassandra_indexed_row_type, name="value",
                                  description="A description")
        
        for attr_key, attr_value in attr_dict.items():
            attr = row_resource.attr.add()
            attr.attribute_name = attr_key
            attr.attribute_value = attr_value
            
        response.configuration = row_resource.ResourceObject    
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
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        cassandra_row =  create_request.CreateObject(cassandra_indexed_row_type)
        for attr_key,attr_value in index_attributes.items():
            attr = cassandra_row.attrs.add()
            attr.attribute_name = attr_key
            attr.attribute_value = attr_value
        
        create_request.configuration = cassandra_row    
        (content, headers, msg) = yield self.rpc_send('query', create_request)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def put(self, key, value, index_attributes={}):
        log.info("Called CassandraInventoryService client")
        
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        cassandra_row =  create_request.CreateObject(cassandra_indexed_row_type)
        cassandra_row.key = key
        cassandra_row.value = value
        for attr_key,attr_value in index_attributes.items():
            attr = cassandra_row.attrs.add()
            attr.attribute_name = attr_key
            attr.attribute_value = attr_value 
        
        create_request.configuration = cassandra_row  
        (content, headers, msg) = yield self.rpc_send('put', create_request)
        defer.returnValue(content)
    
    @defer.inlineCallbacks
    def get(self, key):
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        cassandra_row =  create_request.CreateObject(cassandra_indexed_row_type)
        cassandra_row.key = key
        
        create_request.configuration = cassandra_row  
        (content, headers, msg) = yield self.rpc_send('get',create_request)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def remove(self, key):
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        cassandra_row =  create_request.CreateObject(cassandra_indexed_row_type)
        cassandra_row.key = key
        
        create_request.configuration = cassandra_row  
        (content, headers, msg) = yield self.rpc_send('remove', create_request)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def get_query_attributes(self):
        """
        This request does not send any argument. The message is used as a dummy argument.
        """
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        cassandra_row =  create_request.CreateObject(cassandra_indexed_row_type)
        create_request.configuration = cassandra_row  
        (content, headers, msg) = yield self.rpc_send('get_query_attributes', create_request)
        defer.returnValue(content)
        
        
           
