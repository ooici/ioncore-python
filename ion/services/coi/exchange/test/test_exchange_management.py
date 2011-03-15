#!/usr/bin/env python

"""
@file ion/services/coi/test/test_identity_registry.py
@author Roger Unwin
@brief test service for registering users
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.test.iontest import IonTestCase
from ion.services.coi.exchange.exchange_management import ExchangeManagementClient
from ion.resources import coi_resource_descriptions
from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
 
import ion.services.coi.exchange.resource_wrapper as res_wrapper
from ion.services.coi.exchange.resource_wrapper import ClientHelper

class ExchangeManagementTest(IonTestCase):
    """
    Testing client classes of User Registration
    """

    @defer.inlineCallbacks
    def setUp(self):
        """
        """
        yield self._start_container()
        services = [
            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry',
                'module':'ion.services.coi.resource_registry_beta.resource_registry',
                'class':'ResourceRegistryService',
                'spawnargs':{'datastore_service':'datastore'}
            },
        ]
        self.process = yield self._spawn_processes(services)
        self.emc = ExchangeManagementClient(proc = self.test_sup)
        self.helper = ClientHelper(self.test_sup)
        
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes(self.process)
        yield self._stop_container()



    def test_trivial(self):
        """
        """


    @defer.inlineCallbacks
    def test_trivial_create_resources(self):
        """
        A lower level test to make sure all of our resource definitions are 
        actually usable.  Later on, this code will only be used within the
        boilerplate code.
        """
        rc = ResourceClient(proc=self.test_sup)
        for type in res_wrapper.all_types:
            _name = type + " name"
            _desc = type + " description"
            resource = yield rc.create_instance(res_wrapper.all_types[type], ResourceName=_name, ResourceDescription=_desc)
            self.assertIsInstance(resource, ResourceInstance)
            self.assertEqual(resource.ResourceLifeCycleState, resource.NEW)
            self.assertEqual(resource.ResourceName, _name)
            self.assertEqual(resource.ResourceDescription, _desc)


    @defer.inlineCallbacks
    def test_trivial_store_objects(self):
        """
        A higher level test that ensures all of our resource definitions
        can be used within the boilerplate convenience wrappers.
        """
        for type in res_wrapper.all_types:
            msg = yield self.helper.create_object(res_wrapper.all_types[type])
            if hasattr(msg.configuration,'name'):
                msg.configuration.name = "name"
            if hasattr(msg.configuration,'description'):
                msg.configuration.description = "description"
                                
            id = yield self.emc._create_object(msg)
            
            
            #assert bp.isHash(id)
            



    @defer.inlineCallbacks
    def test_trivial_retrieve_objects(self):
        """
        A higher level test that ensures all of our resource definitions
        can be used within the boilerplate convenience wrappers.
        """
        for type in res_wrapper.all_types:
            #_type = res_wrapper.all_types[type]
            msg = yield self.helper.create_object(res_wrapper.all_types[type])
            if hasattr(msg.configuration,'name'):
                msg.configuration.name = "name"
            if hasattr(msg.configuration,'description'):
                msg.configuration.description = "description"
            
            id = yield self.emc._create_object(msg)
            
            obj1 = msg.MessageObject
            key = id.GPBMessage.key
            obj2 = yield self.emc._get_object(key)
            
            type1 = obj1.GPBMessage.configuration.type
            type2 = obj2.MessageType
            self.assertEqual(type1.object_id, type2.object_id)
            self.assertEqual(type1.version, type2.version)
            log.debug(type1.object_id, type2.object_id, id)


    @defer.inlineCallbacks
    def test_create_exchange_space(self):
        """
        A test that ensures we can define an exchangespace.  Tests 
        for:
            1) successful creation 
            2) failure on no name
            3) failure on duplicate name
        """
        
        # Case 1:  Expect success
        id = yield self.emc.create_exchangespace("TestExchangeSpace", "This is a test!")

        # Case 2:  Fail because of lack of name
        try:
            id = yield self.emc.create_exchangespace("", "This is a test!")
            self.fail("EMS accepted invalid exchangespace.name")
        except:
            pass
        
        # Case 3:  Fail because of duplicate name
        try:
            id = yield self.emc.create_exchangespace("DuplicateName", "This is a test!")
            id = yield self.emc.create_exchangespace("DuplicateName", "This is another test!")
            self.fail("EMS accepted invalid exchangespace.name")
        except Exception, err:
            pass
       

        
    @defer.inlineCallbacks
    def test_create_exchange_name(self):
        """
        A test that ensures we can define an exchangename.  Tests 
        for:
            1) successful creation 
            2) failure on duplicate name
            3) failure on no name
            4) failure on no exchangespace
        """

        id = yield self.emc.create_exchangespace("TestExchangeSpace", "This is a test!")
 

        # Case 1:  Expect success
        id = yield self.emc.create_exchangename("TestExchangeName", "This is a test!", "TestExchangeSpace")


        # Case 2:  Fail because of lack of name
        try:
            id = yield self.emc.create_exchangename("", "Forgot a name")
            self.fail("EMS accepted invalid exchangename.name")
        except:
            pass
#
        # Case 3:  Fail because of duplicate name
        try:
            id = yield self.emc.create_exchangename("DuplicateName", "Object 1", "TestExchangeSpace")
            id = yield self.emc.create_exchangename("DuplicateName", "Object 2", "TestExchangeSpace")
            self.fail("EMS accepted invalid exchangename.name")
        except Exception, err:
            # print err
            pass
        
        # Case 4:  Fail because of missing exchange space
        try:
            id = yield self.emc.create_exchangename("TestExchangeName", "Object 3", "TestExchangeSpace_XYZZY")
            id = yield self.emc.create_exchangename("TestExchangeName", "Object 3", "")
            self.fail("EMS accepted invalid exchangename.exchangespace")
        except Exception, err:
            # print err
            pass
        
        
        
    @defer.inlineCallbacks
    def test_create_queue(self):
        """
        A test that ensures we can define an exchangename.  Tests 
        for:
            1) successful creation 
        """

        # We need an exchangespace and an exchangename
        id = yield self.emc.create_exchangespace("TestExchangeSpace", "This is a test!")
        id = yield self.emc.create_exchangename("TestExchangeName", "This is a test!", "TestExchangeSpace")

        # Case 1:  Expect success
        id = yield self.emc.create_queue(
                            name="TestQueue", 
                            description="This is a test!", 
                            exchangespace="TestExchangeSpace", 
                            exchangename="TestExchangeName",
                            # topic="alt.humar.best-of-usenet"
                    )



    @defer.inlineCallbacks
    def xtest_create_binding(self):
        """
        A test that ensures we can define a binding.  Tests 
        for:
            1) successful creation 
        """

        # We need an exchangespace and an exchangename
        id = yield self.emc.create_exchangespace("TestExchangeSpace", "This is a test!")
        id = yield self.emc.create_exchangename("TestExchangeName", "This is a test!", "TestExchangeSpace")

        # Case 1:  Expect success
        id = yield self.emc.create_binding(
                            name="TestBinding", 
                            description="This is a test!", 
                            exchangespace="TestExchangeSpace", 
                            exchangename="TestExchangeName",
                            queuename="TestQueue",
                            topic="alt.humar.best-of-usenet"
                    )

    @defer.inlineCallbacks
    def xtest_create_queue(self):
        """
        A test that ensures we can define an exchangename.  Tests 
        for:
            1) successful creation 
            2) failure on duplicate name
            3) failure on no name
            4) failure on no exchangespace
        """

        # We need an exchangespace and an exchangename
        id = yield self.emc.create_exchangespace("TestExchangeSpace", "This is a test!")
        id = yield self.emc.create_exchangename("TestExchangeName", "This is a test!", "TestExchangeSpace")
        id = yield self.emc.create_queue(
                            name="TestQueue", 
                            description="This is a test!", 
                            exchangespace="TestExchangeSpace", 
                            exchangename="TestExchangeName",
                            # topic="alt.humar.best-of-usenet"

                    )
