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

import ion.services.coi.exchange.exchange_boilerplate as bp
from ion.services.coi.exchange.exchange_boilerplate import ClientHelper

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
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry_beta.resource_registry',
                'class':'ResourceRegistryService',
                'spawnargs':{'datastore_service':'datastore'}
            },
#            {
#                'name':'exchange_management',
#                'module':'ion.services.coi.exchange.exchange_management',
#                'class':'ExchangeManagementService',
#            },
        ]
        yield self._spawn_processes(services)
        self.emc = ExchangeManagementClient(proc = self.test_sup)
        self.helper = ClientHelper(self.test_sup)
        
        
    @defer.inlineCallbacks
    def tearDown(self):
        # yield self.exchange_registry_client.clear_exchange_registry()
        yield self._stop_container()


    def xtest_trivial_create_resources(self):
        """
        A lower level test to make sure all of our resource definitions are 
        actually usable.  Later on, this code will only be used within the
        boilerplate code.
        """
        rc = ResourceClient(proc=self.test_sup)
        for type in bp.all_types:
            _name = type + " name"
            _desc = type + " description"
            resource = yield rc.create_instance(bp.all_types[type], name=_name, description=_desc)
            self.assertIsInstance(resource, ResourceInstance)
            self.assertEqual(resource.ResourceLifeCycleState, resource.NEW)
            self.assertEqual(resource.ResourceName, _name)
            self.assertEqual(resource.ResourceDescription, _desc)


    @defer.inlineCallbacks
    def xtest_trivial_store_objects(self):
        """
        A higher level test that ensures all of our resource definitions
        can be used within the boilerplate convenience wrappers.
        """
        for type in bp.all_types:
            msg = yield self.helper.create_object(bp.all_types[type])
            msg.configuration.name = "name"
            msg.configuration.description = "description"
            id = yield self.emc._create_object(msg)
            assert bp.isHash(id) 



    @defer.inlineCallbacks
    def xtest_trivial_retrieve_objects(self):
        """
        A higher level test that ensures all of our resource definitions
        can be used within the boilerplate convenience wrappers.
        """
        for type in bp.all_types:
            _type = bp.all_types[type]
            msg = yield self.helper.create_object(bp.all_types[type])
            msg.configuration.name = "name"
            msg.configuration.description = "description"

            id = yield self.emc._create_object(msg)

            obj1 = msg.MessageObject
            key = id.GPBMessage.key
            obj2 = yield self.emc._get_object(key)

            type1 = obj1.GPBMessage.configuration.type
            type2 = obj2.MessageType
            self.assertEqual(type1.object_id, type2.object_id)
            self.assertEqual(type1.version, type2.version)
            print type1.object_id, type2.object_id, id

    @defer.inlineCallbacks
    def xtest_create_exchange_space(self):
        """
        A test that ensures we can define an exchangespace.  Tests 
        for:
            1) successful creation 
            2) failure on no name
            3) failure on duplicate name
        """
        
        # Case 1:  Expect success
        msg = yield self.helper.create_object(bp.exchangespace_type)
        msg.configuration.name = "TestExchangeSpace"
        msg.configuration.description = "This is a test!"
        id = yield self.emc.create_exchangespace(msg)

        # Case 2:  Fail because of lack of name
        try:
            msg = yield self.helper.create_object(bp.exchangespace_type)
            msg.configuration.description = "This is a test!"
            id = yield self.emc.create_exchangespace(msg)
            self.fail("EMS accepted invalid exchangespace.name")
        except:
            pass
        
        # Case 3:  Fail because of duplicate name
        try:
            msg1 = yield self.helper.create_object(bp.exchangespace_type)
            msg1.configuration.name = "TestDuplicateExchangeSpace"
            msg1.configuration.description = "This is a test!"
            id = yield self.emc.create_exchangespace(msg1)

            msg2 = yield self.helper.create_object(bp.exchangespace_type)
            msg2.configuration.name = "TestDuplicateExchangeSpace"
            msg2.configuration.description = "This is a test of a another type!"
            id = yield self.emc.create_exchangespace(msg2)
            
            self.fail("EMS accepted invalid exchangespace.name")
        except Exception, err:
            pass
       


    @defer.inlineCallbacks
    def xtest_create_exchange_name(self):
        """
        A test that ensures we can define an exchangename.  Tests 
        for:
            1) successful creation 
            2) failure on duplicate name
            3) failure on no name
            4) failure on no exchangespace
        """
        msg = yield self.helper.create_object(bp.exchangespace_type)
        msg.configuration.name = "TestExchangeSpace"
        msg.configuration.description = "This is a test!"
        id = yield self.emc.create_exchangespace(msg)

        # Case 1:  Expect success
        msg = yield self.helper.create_object(bp.exchangename_type)
        msg.configuration.name = "TestExchangeName"
        msg.configuration.description = "This is a test!"
        msg.configuration.exchangespace = "TestExchangeSpace"
        id = yield self.emc.create_exchangename(msg)


        # Case 2:  Fail because of lack of name
        try:
            msg = yield self.helper.create_object(bp.exchangename_type)
            msg.configuration.description = "This is a test!"
            msg.configuration.exchangespace = "TestExchangeSpace"
            id = yield self.emc.create_exchangename(msg)
            self.fail("EMS accepted invalid exchangename.name")
        except:
            pass

        # Case 3:  Fail because of duplicate name
        try:
            msg1 = yield self.helper.create_object(bp.exchangename_type)
            msg1.configuration.name = "TestDuplicateExchangeName"
            msg1.configuration.exchangespace = "TestExchangeSpace"
            msg1.configuration.description = "This is a test!"
            id = yield self.emc.create_exchangename(msg1)

            msg2 = yield self.helper.create_object(bp.exchangename_type)
            msg2.configuration.name = "TestDuplicateExchangeName"
            msg2.configuration.exchangespace = "TestExchangeSpace"
            msg2.configuration.description = "This is a test of a another type!"
            id = yield self.emc.create_exchangename(msg2)
            
            self.fail("EMS accepted invalid exchangename.name")
        except Exception, err:
            # print err
            pass
        
        # Case 3:  Fail because of duplicate name
        try:
            msg = yield self.helper.create_object(bp.exchangename_type)
            msg.configuration.name = "TestBad"
            msg.configuration.description = "This is a test!"
            msg.configuration.exchangespace = "TestExchangeSpaceTypo"
            id = yield self.emc.create_exchangename(msg)
        except Exception, err:
            # print err
            pass
