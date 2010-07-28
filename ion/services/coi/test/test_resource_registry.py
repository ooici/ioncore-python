#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@author David Stuebe
@brief test service for registering resources and client classes
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.data.datastore import registry
from ion.resources import coi_resource_descriptions

from ion.services.coi.resource_registry import ResourceRegistryClient
from ion.test.iontest import IonTestCase
from ion.data import dataobject


class ResourceRegistryTest(IonTestCase):
    """
    Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._start_core_services()
        services = [
            {'name':'resourceregistry1','module':'ion.services.coi.resource_registry','class':'ResourceRegistryService'}]
        sup = yield self._spawn_processes(services)

        self.rrc = ResourceRegistryClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self.rrc.clear_registry
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_resource_reg(self):

        # Pick a resource class object to put in the registry
        res_to_describe = coi_resource_descriptions.IdentityResource
        
        # Registration creates the resource description from the class object
        res_description = yield self.rrc.register_resource_definition(res_to_describe)
        # the act of registration also registers the resources from which it inherits
                
        # show that Identity, which is a stateful resource also caused StatefulResource to be registerd
        stateful_description = yield self.rrc.find_registered_resource_definition_from_resource(dataobject.StatefulResource)
             
        # from the StatefulResource description extract the reference to its base class, Resource
        self.assertEqual(len(stateful_description.inherits_from),1)
        ref_to_resource = stateful_description.inherits_from[0]
        
        # Get the description of the class Resource
        resource_description = yield self.rrc.get_resource_definition(ref_to_resource)
        
        # change an attribute of resource and check it back in
        resource_description.name = 'Testing changes!'
        
        # you can use the same interface to overwrite or change an existing description
        res_description = yield self.rrc.register_resource_definition(resource_description)        
        

    @defer.inlineCallbacks
    def test_resource_instance_reg(self):
        # Create an instance to register
        res_inst = coi_resource_descriptions.IdentityResource.create_new_resource()
        
        # Create an owner identity to register the instance with.
        # this should be in an identity registry before being used... 
        me = coi_resource_descriptions.IdentityResource.create_new_resource()
        me.name = 'david'
        me.ooi_id = 'just a programmer...'
        
        # Register the instance with 'me' as the owner
        instance = yield self.rrc.register_resource_instance(res_inst,me)
        
        # Show that registration of the instance also resulted in registration of its description
        resource_description = yield self.rrc.get_resource_definition(instance.description)
        
    @defer.inlineCallbacks
    def test_describe_resource(self):        
        # Test a simple, non-recursive resource description
        rd = yield self.rrc.describe_resource(dataobject.Resource)
        self.assertEqual(rd.name, 'Resource')
        
        



class ResourceRegistryCoreServiceTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.sup = yield self._start_core_services()
        #logging.info('self.sup.proc_state'+str(self.sup.proc_state))
        

    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self.rrc.clear_registry
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_reg_startup(self):
        self.rrc = ResourceRegistryClient(proc=self.sup)
        
        # Show that the registry work when started as a core service
        res_to_describe = coi_resource_descriptions.IdentityResource
        res_description = yield self.rrc.register_resource_definition(res_to_describe)
        
        #print res_description
        
