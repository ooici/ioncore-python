#!/usr/bin/env python

"""
@file ion/services/dm/preservation/test/test_cassandra_manager_service.py
@author Matt Rodriguez
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.core.messaging.message_client import MessageClient
from ion.services.dm.inventory.inventory_service import CassandraInventoryClient
from ion.core.object import object_utils

from ion.core import ioninit
CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

cassandra_keyspace_type = object_utils.create_type_identifier(object_id=2506, version=1)
cassandra_column_family_type = object_utils.create_type_identifier(object_id=2507, version=1)
cassandra_request_type = object_utils.create_type_identifier(object_id=2510, version=1)
resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)
columndef_type = object_utils.create_type_identifier(object_id=2508, version=1)

class CassandraInventoryTester(IonTestCase):
    
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
             'module': 'ion.services.dm.inventory.inventory_service',
             'class':'CassandraManagerAgent'},         
        ]
        sup = yield self._spawn_processes(services)
        self.client = CassandraInventoryClient(proc=sup)
        self.keyspace = 'ManagerServiceKeyspace'
        self.mc = MessageClient(proc = self.test_sup)
        

    @defer.inlineCallbacks
    def tearDown(self):
        log.info("In tearDown")
        yield self._shutdown_processes()
        yield self._stop_container()
        
    
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
    
  
