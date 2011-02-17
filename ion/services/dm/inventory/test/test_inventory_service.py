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


resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)
cassandra_indexed_row_type = object_utils.create_type_identifier(object_id=2511, version=1)

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
            {'name': 'inventory',
             'module': 'ion.services.dm.inventory.inventory_service',
             'class':'CassandraInventoryService'},         
        ]
        sup = yield self._spawn_processes(services)
        self.client = CassandraInventoryClient(proc=sup)
        self.mc = MessageClient(proc = self.test_sup)
        

    @defer.inlineCallbacks
    def tearDown(self):
        log.info("In tearDown")
        yield self._shutdown_processes()
        yield self._stop_container()
        
    
    def test_instantiation_only(self):
        pass

    #@itv(CONF)
    @defer.inlineCallbacks
    def test_put_rows(self):
        create_request = yield self.mc.create_instance(resource_request_type, name='Creating a create_request')
        row =  create_request.CreateObject(cassandra_indexed_row_type)
        row.key = "Key1"
        row.value = "Value1"
        attr_dict = {"Subject":"Who", "Predicate":"Descriptive Verb", "Object": "The thing you're looking for"}
        for key,value in attr_dict.items():
            attr = row.attrs.add()
            attr.attribute_name = key
            attr.attribute_value = value    
        
        create_request.configuration = row
        create_response = yield self.client.put(create_request)   
        log.info(create_response.result) 
  
