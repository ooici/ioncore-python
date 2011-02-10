#!/usr/bin/env python

"""
@file ion/services/dm/preservation/test/test_cassandra_manager_service.py
@author David Stuebe
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.core.messaging.message_client import MessageClient
from ion.services.dm.preservation.cassandra_manager_agent import CassandraManagerClient
from ion.core.object import object_utils

from ion.core import ioninit
CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

cassandra_keyspace_type = object_utils.create_type_identifier(object_id=2506, version=1)
cassandra_column_family_type = object_utils.create_type_identifier(object_id=2507, version=1)
cassandra_request_type = object_utils.create_type_identifier(object_id=2510, version=1)
resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)
columndef_type = object_utils.create_type_identifier(object_id=2508, version=1)

class CassandraManagerTester(IonTestCase):
    
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 30
        services = [
           {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
           {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {'name': 'cassandra_manager_agent',
             'module': 'ion.services.dm.preservation.cassandra_manager_agent',
             'class':'CassandraManagerAgent'},         
        ]
        sup = yield self._spawn_processes(services)
        self.client = CassandraManagerClient(proc=sup)
        self.keyspace = 'ManagerServiceKeyspace'
        self.mc = MessageClient(proc = self.test_sup)
        

    @defer.inlineCallbacks
    def tearDown(self):
        log.info("In tearDown")
        yield self._shutdown_processes()
        yield self._stop_container()
        
    @itv(CONF)
    def test_instantiation_only(self):
        pass

    @itv(CONF)
    @defer.inlineCallbacks
    def test_create_archive(self):
        """
        This integration test does not remove the keyspace from the Cassandra cluster. Do not run 
        it unless you know how to delete the keyspace using another client. 
        """
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        create_request.configuration =  create_request.CreateObject(cassandra_keyspace_type)

        #persistent_archive_repository, cassandra_keyspace  = self.wb.init_repository(cassandra_keyspace_type)
        create_request.configuration.name = self.keyspace
        log.info("create_request.configuration " + str(create_request))
    
        create_response = yield self.client.create_persistent_archive(create_request)
        log.info("create_response.result " + str(create_response.result))
        self.failUnlessEqual(create_response.result, "Created")
    
    @itv(CONF)
    @defer.inlineCallbacks
    def test_create_cache(self):
        create_request = yield self.mc.create_instance(cassandra_request_type, name='Creating a create_request')
        create_request.persistent_archive = create_request.CreateObject(cassandra_keyspace_type)
        create_request.cache_configuration = create_request.CreateObject(cassandra_column_family_type)
        
        create_request.persistent_archive.name = self.keyspace
        create_request.cache_configuration.name = "SomeCF"
        
        create_response = yield self.client.create_cache(create_request)
        log.info("create_response.result " + str(create_response.result))
        self.failUnlessEqual(create_response.result, "Created")
        
    @itv(CONF)    
    @defer.inlineCallbacks
    def test_update_archive(self):
        
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        create_request.configuration =  create_request.CreateObject(cassandra_keyspace_type)

        create_request.configuration.name = self.keyspace
        log.info("create_request.configuration " + str(create_request))
    
        create_response = yield self.client.create_persistent_archive(create_request)
        self.failUnlessEqual(create_response.result, "Created")
        
        log.info("create_response " + str(create_response))
        update_request = yield self.mc.create_instance(resource_request_type, name='Creating an update_request')
        update_request.configuration =  create_request.configuration
        update_request.configuration.name = 'ManagerServiceKeyspace' 
        update_request.configuration.strategy_class='org.apache.cassandra.locator.SimpleStrategy'
        update_request.configuration.replication_factor = 2
        
        update_request.resource_reference = create_response.resource_reference
        log.info("Sending delete_request")
        
        update_response = yield self.client.update_persistent_archive(update_request)
        log.info("update_response.result " + str(update_response.result))
        self.failUnlessEqual(update_response.result, "Updated")
        
        delete_request = yield self.mc.create_instance(resource_request_type, name='Creating a delete_request')
        delete_request.configuration =  create_request.configuration
        
        delete_request.resource_reference = create_response.resource_reference
        log.info("Sending delete_request")
        
        delete_response = yield self.client.delete_persistent_archive(delete_request)
        log.info("delete_response.result " + str(delete_response.result))
        self.failUnlessEqual(delete_response.result, "Deleted")
    
    #@itv(CONF)
    @defer.inlineCallbacks
    def test_update_cache(self):
        create_request = yield self.mc.create_instance(cassandra_request_type, name='Creating a create_request')
        create_request.persistent_archive = create_request.CreateObject(cassandra_keyspace_type)
        create_request.cache_configuration = create_request.CreateObject(cassandra_column_family_type)
        
        create_request.persistent_archive.name = self.keyspace
        create_request.cache_configuration.name = "SomeCF"
        
        create_response = yield self.client.create_cache(create_request)
        log.info("create_response.result " + str(create_response.result))
        log.info("create_response.resource_reference :" + str(create_response.resource_reference))
        self.failUnlessEqual(create_response.result, "Created") 
        
        update_request = yield self.mc.create_instance(cassandra_request_type, name='Creating an update_request')
        update_request.persistent_archive = create_request.persistent_archive
        update_request.cache_configuration =  create_request.cache_configuration
        update_request.resource_reference = create_response.resource_reference
        
        column = create_request.CreateObject(columndef_type)
        column.column_name = "state"
        column.validation_class = 'org.apache.cassandra.db.marshal.UTF8Type'
        #IndexType.KEYS is 0, and IndexType is an enum
        column.index_type = 0
        column.index_name = 'stateIndex'
        update_request.cache_configuration.column_metadata.add()
        update_request.cache_configuration.column_metadata[0] = column
        
        log.info("update_request.resource_reference: " + str(update_request.resource_reference))
        
        update_response = yield self.client.update_cache(update_request)
        self.failUnlessEqual(update_response.result, "Updated") 
        
        delete_request = yield self.mc.create_instance(cassandra_request_type, name='Creating a delete_request')
        delete_request.persistent_archive = create_request.persistent_archive
        delete_request.cache_configuration =  create_request.cache_configuration
        
        delete_request.resource_reference = create_response.resource_reference
        log.info("Sending delete_request")
        
        delete_response = yield self.client.delete_cache(delete_request)
        log.info("delete_response.result " + str(delete_response.result))
        self.failUnlessEqual(delete_response.result, "Deleted")

        
        
     
    @itv(CONF)   
    @defer.inlineCallbacks
    def test_delete_archive(self):
                
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        create_request.configuration =  create_request.CreateObject(cassandra_keyspace_type)

        create_request.configuration.name = self.keyspace
        log.info("create_request.configuration " + str(create_request))
    
        create_response = yield self.client.create_persistent_archive(create_request)
        log.info("create_response " + str(create_response))
        self.failUnlessEqual(create_response.result, "Created")
        
        delete_request = yield self.mc.create_instance(resource_request_type, name='Creating a delete_request')
        delete_request.configuration =  create_request.configuration
        
        delete_request.resource_reference = create_response.resource_reference
        log.info("Sending delete_request")
        
        delete_response = yield self.client.delete_persistent_archive(delete_request)
        log.info("delete_response.result " + str(delete_response.result))
        self.failUnlessEqual(delete_response.result, "Deleted")
