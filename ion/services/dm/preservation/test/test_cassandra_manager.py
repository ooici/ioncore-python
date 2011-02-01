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

cassandra_keyspace_type = object_utils.create_type_identifier(object_id=2506, version=1)
resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)


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
        #self.wb = workbench.WorkBench('No Process: Testing only')
        self.mc = MessageClient(proc = self.test_sup)
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_instantiation_only(self):
        pass


    @defer.inlineCallbacks
    def test_create_archive(self):
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        create_request.configuration =  create_request.CreateObject(cassandra_keyspace_type)

        #persistent_archive_repository, cassandra_keyspace  = self.wb.init_repository(cassandra_keyspace_type)
        create_request.configuration.name = 'ManagerServiceKeyspace'
        log.info("create_request.configuration " + str(create_request))
    
        create_response = yield self.client.create_persistent_archive(create_request)
        log.info("create_response.result " + str(create_response.result))
        
        
    @defer.inlineCallbacks
    def test_update_archive(self):
        
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        create_request.configuration =  create_request.CreateObject(cassandra_keyspace_type)

        create_request.configuration.name = 'ManagerServiceKeyspace'
        log.info("create_request.configuration " + str(create_request))
    
        create_response = yield self.client.create_persistent_archive(create_request)
        log.info("create_response " + str(create_response))
        update_request = yield self.mc.create_instance(resource_request_type, name='Creating a delete_request')
        update_request.configuration =  create_request.configuration
        update_request.configuration.name = 'ManagerServiceKeyspace'
        update_request.configuration.replication_factor = 2
        
        update_request.resource_reference = create_response.resource_reference
        log.info("Sending delete_request")
        
        update_response = yield self.client.update_persistent_archive(update_request)
        log.info("update_response.result " + str(update_response.result))
        
    @defer.inlineCallbacks
    def test_delete_archive(self):
                
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        create_request.configuration =  create_request.CreateObject(cassandra_keyspace_type)

        create_request.configuration.name = 'ManagerServiceKeyspace'
        log.info("create_request.configuration " + str(create_request))
    
        create_response = yield self.client.create_persistent_archive(create_request)
        log.info("create_response " + str(create_response))
        delete_request = yield self.mc.create_instance(resource_request_type, name='Creating a delete_request')
        delete_request.configuration =  create_request.configuration
        
        delete_request.resource_reference = create_response.resource_reference
        log.info("Sending delete_request")
        
        delete_response = yield self.client.delete_persistent_archive(delete_request)
        log.info("delete_response.result " + str(delete_response.result))
