#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry_beta/test/test_resource_registry.py
@author Michael Meisinger
@author David Stuebe
@brief test service for registering resources and client classes
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from net.ooici.core.type import type_pb2
from net.ooici.play import addressbook_pb2
from ion.core.object import gpb_wrapper

from ion.services.coi.resource_registry_beta.resource_registry import ResourceRegistryClient
from ion.test.iontest import IonTestCase


class ResourceRegistryTest(IonTestCase):
    """
    Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._start_core_services()
        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService'}]
        sup = yield self._spawn_processes(services)

        self.rrc = ResourceRegistryClient(proc=sup)
        self.sup = sup

    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_resource_reg(self):
        
        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        proc_ds1 = self._get_procinstance(child_ds1)
        
        child_rr1 = yield self.sup.get_child_id('resource_registry1')
        log.debug('Process ID:' + str(child_rr1))
        proc_rr2 = self._get_procinstance(child_rr1)
        
        # Replicate what we should receive from the requesting service
        wb = proc_rr2.workbench
        repo, type_obj = wb.init_repository(rootclass=type_pb2.GPBType)
        repo._set_type_from_obj(type_obj, addressbook_pb2.AddressLink)
        
        # Test the business logic of the register resource instance operation
        res_id = yield proc_rr2._register_resource_instance(type_obj)
            
        # Check the result!
        new_repo = proc_ds1.workbench.get_repository(res_id)
            
        resource = new_repo.checkout('master')
            
        self.assertEqual(resource.identity, res_id)
        
        self.assertEqual(resource.resource_object.GPBType, type_obj.GPBMessage)
        
        
        
        
        
        

#
#class ResourceRegistryCoreServiceTest(IonTestCase):
#    @defer.inlineCallbacks
#    def setUp(self):
#        yield self._start_container()
#        self.sup = yield self._start_core_services()
#        #log.info('self.sup.proc_state'+str(self.sup.proc_state))
#        
#
#    @defer.inlineCallbacks
#    def tearDown(self):
#        # You must explicitly clear the registry in case cassandra is used as a back end!
#        yield self.rrc.clear_registry
#        yield self._stop_container()
#
#    @defer.inlineCallbacks
#    def test_reg_startup(self):
#        self.rrc = ResourceRegistryClient(proc=self.sup)
#        
#        # Show that the registry work when started as a core service
#        res_to_describe = coi_resource_descriptions.IdentityResource
#        res_description = yield self.rrc.register_resource_definition(res_to_describe)
#        
#        #print res_description
#        
