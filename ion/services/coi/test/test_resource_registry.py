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
        yield self.rrc.clear_registry
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_resource_reg(self):
        # put in a bogus resource for now...
        res_to_describe = coi_resource_descriptions.IdentityResource
        res_description = yield self.rrc.register_resource_definition(res_to_describe)
                
                
        stateful_description = yield self.rrc.find_resource_definition_from_resource(dataobject.StatefulResource)
             
        self.assertEqual(len(stateful_description.inherits_from),1)
        ref_to_resource = stateful_description.inherits_from[0]
        
        resource_description = yield self.rrc.get_resource_definition(ref_to_resource)
        

    @defer.inlineCallbacks
    def test_resource_instance_reg(self):
        res_inst = coi_resource_descriptions.IdentityResource.create_new_resource()
        
        # this should be in an identity registry before being used... 
        me = coi_resource_descriptions.IdentityResource.create_new_resource()
        me.name = 'david'
        me.ooi_id = 'just a programmer...'
        
        
        instance = yield self.rrc.register_resource_instance(res_inst,me)
        
        resource_description = yield self.rrc.get_resource_definition(instance.description)
        
        print instance
        print resource_description
        
        
        
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
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_reg_startup(self):
        self.rrc = ResourceRegistryClient(proc=self.sup)
