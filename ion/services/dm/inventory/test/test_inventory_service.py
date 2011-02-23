#!/usr/bin/env python

"""
@file ion/services/dm/preservation/test/test_cassandra_manager_service.py
@author Matt Rodriguez
"""
from ion.util import procutils as pu    
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
             'class':'CassandraInventoryService'}         
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
        log.info("In test_instantiation_only")
        
    #@itv(CONF)    
    @defer.inlineCallbacks
    def test_put_rows(self):
        log.info("In test_put_rows")
        
        key = "Key1"
        value = "Value1"
        attr_dict = {"Subject":"Who", "Predicate":"Descriptive Verb", "Object": "The thing you're looking for"}
        put_response = yield self.client.put(key,value,attr_dict)   
        log.info(create_response.result) 
        self.failUnlessEqual(put_response.result, "Put complete")
         
    @defer.inlineCallbacks
    def test_query(self): 
        key1 = "Key1"
        value1 = "Value1"
        attr_dict1 = {"Subject":"Me", "Predicate":"Descriptive Verb", "Object": "The first thing you're looking for"}
        key2 = "Key2"
        value2 = "Value2"
        attr_dict2 = {"Subject":"You", "Predicate":"Descriptive Verb", "Object": "The thing you're looking for"}
        key3 = "Key3"
        value3 = "Value3"
        attr_dict3 = {"Subject":"Me", "Predicate":"Descriptive Verb", "Object": "The second thing you're looking for"}
        put_response1 = yield self.client.put(key1, value1, attr_dict1)
        put_response2 = yield self.client.put(key2, value2, attr_dict2)
        put_response3 = yield self.client.put(key3, value3, attr_dict3)
        index_attrs = {"Subject":"Me"}
        create_response = yield self.client.query(index_attrs)
          
    @defer.inlineCallbacks
    def test_get(self):
        key = "Key1"
        value = "Value1"
        attr_dict = {"Subject":"Who", "Predicate":"Descriptive Verb", "Object": "The thing you're looking for"} 
        put_response = yield self.client.put(key,value,attr_dict)
        get_response = yield self.client.get(key)
        self.failUnlessEqual(get_response.configuration.value, value)
        
        
    @defer.inlineCallbacks
    def test_remove(self):
        key = "Key1"
        value = "Value1"
        attr_dict = {"Subject":"Who", "Predicate":"Descriptive Verb", "Object": "The thing you're looking for"}
        put_response = yield self.client.put(key,value,attr_dict)  
        remove_response = yield self.client.remove(key)
        get_response = yield self.client.get(key)
        log.info(get_response.configuration.value)
        self.failUnlessEqual(get_response.configuration.value,"")
        
        
    @defer.inlineCallbacks
    def test_get_query_attributes(self):
        query_attributes_response = yield self.client.get_query_attributes()
        cassandra_row = query_attributes_response.configuration
        index_attrs = []
        for attr in cassandra_row.attrs:
            log.info(attr.attribute_name)
            index_attrs.append(attr.attribute_name)
            
        correct_set = set(["Subject", "Predicate", "Object"])
        index_attrs_set = set(index_attrs)
        self.failUnlessEqual(correct_set, index_attrs_set)
        
