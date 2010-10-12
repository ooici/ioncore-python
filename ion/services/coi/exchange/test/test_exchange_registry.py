#!/usr/bin/env python

"""
@file ion/services/coi/test/test_identity_registry.py
@author Roger Unwin
@brief test service for registering users
"""

import ion.util.ionlog
import sys,os
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.test.iontest import IonTestCase
from ion.services.coi.exchange.exchange_registry import ExchangeRegistryClient

from ion.resources.coi_resource_descriptions import \
    ExchangeName,       \
    AMQPMapping,        \
    HardwareMapping,    \
    BrokerCredentials,  \
    BrokerFederation,   \
    ExchangeSpace

class ExchangeClientTest(IonTestCase):
    """
    Testing client classes of User Registration
    """

    @defer.inlineCallbacks
    def setUp(self):
        """
        """
        yield self._start_container()

        services = [{'name':'exchange_registry','module':'ion.services.coi.exchange.exchange_registry','class':'ExchangeRegistryService'}]
        supervisor = yield self._spawn_processes(services)
        self.exchange_registry_client = ExchangeRegistryClient(proc=supervisor)

    @defer.inlineCallbacks
    def tearDown(self):
        # yield self.exchange_registry_client.clear_exchange_registry()
        yield self._stop_container()


    def make_mapping(self, type, values):
        """
        Helper function.  
        Rather than repeat monotonous resource creation code, this function will fill
        out the resource (type) from a provided dictionary (values).
        
        Without this helper, the logic would be much more hardcoded such as:
        myresource = MyResourseClass.create_new_resource()
        myresource.myattr1 = myval1
        myresource.myattr2 = myval2
        ...
        """
        mapping = {
            'hardwaremapping'   : lambda : HardwareMapping.create_new_resource(),
            'amqpmapping'       : lambda : AMQPMapping.create_new_resource(),
            'exchangename'      : lambda : ExchangeName.create_new_resource(),
            'brokercredentials' : lambda : BrokerCredentials.create_new_resource(),
            'brokerfederation'  : lambda : BrokerFederation.create_new_resource(),
            'exchangespace'     : lambda : ExchangeSpace.create_new_resource()
        }[type]()
        for key in values:
            cmd = "mapping.%s = values['%s']"%(key,key)
            exec cmd in locals()
        return mapping




    # SIMPLE RESOURCE OPERATIONS
    # SIMPLE RESOURCE OPERATIONS
    # SIMPLE RESOURCE OPERATIONS
    
    @defer.inlineCallbacks
    def test_amqpmapping_resource(self):
        """
        """

        for p in sys.path:
            e = os.path.exists(p)
            log.debug(p + " : " + str(e))

        values = {
                'name'        : 'AMQP Mapping Test',
                'description' : "This AMQP Mapping is part of a unit test"
        }
        mapping = self.make_mapping('amqpmapping', values)
        registered = yield self.exchange_registry_client.register_amqpmapping(mapping)
        retrieved = yield self.exchange_registry_client.get_exchangename_by_id(registered.RegistryIdentity)
        
        tofind = AMQPMapping.create_new_resource()
        tofind.name = values['name']
        
        found = yield self.exchange_registry_client.find_amqpmapping(tofind,regex=True,ignore_defaults=True,attnames=[AMQPMapping.name])
        print found 
        print tofind
        print '----'
        
        self.assertEquals(registered.RegistryIdentity,retrieved.RegistryIdentity)


    @defer.inlineCallbacks
    def test_hardwaremapping_resource(self):
        """
        """
        values = {
                'name'        : 'Hardware Mapping Test',
                'description' : "This Hardware Mapping is part of a unit test"
        }
        mapping = self.make_mapping('hardwaremapping', values)
        registered = yield self.exchange_registry_client.register_hardwaremapping(mapping)
        retrieved = yield self.exchange_registry_client.get_hardwaremapping_by_id(registered.RegistryIdentity)
        self.assertEquals(registered.RegistryIdentity,retrieved.RegistryIdentity)


    @defer.inlineCallbacks
    def test_exchangename_resource(self):
        """
        Trivial test to verify that we can insert an ExchangeName resource into
        our data store.  Note that the nested resources (AMQPMapping and
        HardwareMapping) are left for complex tests below.
        """

        values = {
                  'name' :        "Exchange Name Test",
                  'description' : "This exchange name is part of a unit test"
        }
        mapping = self.make_mapping('exchangename', values)
        registered = yield self.exchange_registry_client.register_exchangename(mapping)
        retrieved = yield self.exchange_registry_client.get_exchangename_by_id(registered.RegistryIdentity)
        self.assertEquals(registered.RegistryIdentity,retrieved.RegistryIdentity)


    @defer.inlineCallbacks
    def test_brokerfederation_resource(self):
        """
        Trivial test to verify that we can insert an ExchangeName resource into
        our data store.  Note that the nested resources (AMQPMapping and
        HardwareMapping) are left for complex tests below.
        """

        values = {
                  'name' :        "Broker Federation Test",
                  'description' : "This broker federation is part of a unit test"
        }
        mapping = self.make_mapping('brokerfederation', values)
        registered = yield self.exchange_registry_client.register_brokerfederation(mapping)
        retrieved = yield self.exchange_registry_client.get_brokerfederation_by_id(registered.RegistryIdentity)
        self.assertEquals(registered.RegistryIdentity,retrieved.RegistryIdentity)



    @defer.inlineCallbacks
    def test_brokercredentials_resource(self):
        """
        Trivial test to verify that we can insert an ExchangeName resource into
        our data store.  Note that the nested resources (AMQPMapping and
        HardwareMapping) are left for complex tests below.
        """

        values = {
                  'name' :        "Broker Credentials Test",
                  'description' : "These broker credentials are part of a unit test"
        }
        mapping = self.make_mapping('brokercredentials', values)
        result = yield self.exchange_registry_client.register_amqpmapping(mapping)


    @defer.inlineCallbacks
    def test_register_complex_exchangename(self):
        """
        """
        aname = "AMQP Mapping Test"
        adesc = "This AMQP Mapping is part of a unit test"
        amap = AMQPMapping.create_new_resource()
        amap.name = aname
        amap.description = adesc


        hmap = HardwareMapping.create_new_resource()
        hmap.name = "Hardware Mapping Test"
        hmap.description = "This Hardware Mapping is part of a unit test"

        exchangename = ExchangeName.create_new_resource()
        exchangename.name = "Exchange Name Test"
        exchangename.description = "This Exchange Name is part of a unit test"
        exchangename.amqpmapping = amap
        exchangename.hardwaremapping = hmap
        result = yield self.exchange_registry_client.register_exchangename(exchangename)
