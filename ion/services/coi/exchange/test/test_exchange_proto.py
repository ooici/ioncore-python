#!/usr/bin/env python

"""
@file ion/services/coi/exchange/test/test_exchange_proto.py
@author Brian Fox
@brief Test google buffer protocol exchange management definitions
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.object import gpb_wrapper
from ion.core.object import workbench
from ion.core.object import object_utils

from ion.services.coi.resource_registry_beta.resource_registry import ResourceRegistryClient, ResourceRegistryError
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.test.iontest import IonTestCase


proto_types = {}
proto_types['ExchangeSpace'] = object_utils.create_type_identifier(object_id=1001, version=1)
proto_types['ExchangeName'] = object_utils.create_type_identifier(object_id=1002, version=1)
proto_types['Queue'] = object_utils.create_type_identifier(object_id=1003, version=1)
proto_types['Binding'] = object_utils.create_type_identifier(object_id=1004, version=1)
proto_types['AMQPExchangeMapping'] = object_utils.create_type_identifier(object_id=1005, version=1)
proto_types['AMQPQueueMapping'] = object_utils.create_type_identifier(object_id=1006, version=1)
proto_types['AMQPBrokerCredentials'] = object_utils.create_type_identifier(object_id=1007, version=1)
proto_types['BrokerFederation'] = object_utils.create_type_identifier(object_id=1008, version=1)
proto_types['HardwareMapping'] = object_utils.create_type_identifier(object_id=1009, version=1)
invalid_type = object_utils.create_type_identifier(object_id=-1, version=1)

class ExchangeProtoTest(IonTestCase):
    """
    Tests the trivial creation of all ExchangeManagement resources.  
    More indepth requirements (such as required fields and name conflicts) 
    are left to the ExchangeRegistryTest.
    """
        
        
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 40
        yield self._start_container()
        #self.sup = yield self._start_core_services()
        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}}]
        sup = yield self._spawn_processes(services)
            
        self.rrc = ResourceRegistryClient(proc=self.test_sup)
        self.rc = ResourceClient(proc=sup)
        self.sup = sup
        
    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()
        
    @defer.inlineCallbacks
    def test_create_resources(self):
                
        for type in proto_types:
            _name = type + " name"
            _desc = type + " description"
            resource = yield self.rc.create_instance(proto_types[type], name=_name, description=_desc)
            self.assertIsInstance(resource, ResourceInstance)
            self.assertEqual(resource.ResourceLifeCycleState, resource.NEW)
            self.assertEqual(resource.ResourceName, _name)
            self.assertEqual(resource.ResourceDescription, _desc)
        
