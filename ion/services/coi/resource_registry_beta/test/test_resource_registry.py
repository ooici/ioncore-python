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
from net.ooici.services.coi import resource_framework_pb2

from ion.core.exception import ReceivedError

from ion.core.object import object_utils
from ion.core.object import gpb_wrapper

from ion.services.coi.resource_registry_beta.resource_registry import ResourceRegistryClient
from ion.test.iontest import IonTestCase

from ion.core.object import object_utils
RESOURCE_DESCRIPTION_TYPE = object_utils.create_type_identifier(object_id=1101, version=1)
IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)


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
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}}]
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
        
        # Create a sendable resource object
        description_repository, resource_description = wb.init_repository(root_type=RESOURCE_DESCRIPTION_TYPE)
        
        # Set the description
        resource_description.name = 'Johns resource'
        resource_description.description = 'Lots of metadata'
        object_utils.set_type_from_obj(addressbook_pb2.AddressLink, resource_description.object_type)
        
        res_type = description_repository.create_object(IDREF_TYPE)
        res_type.key = 'Some junk'
        resource_description.resource_type = res_type

        
        # Test the business logic of the register resource instance operation
        result = yield proc_rr2._register_resource_instance(resource_description)
            
        if result.MessageResponseCode == result.ResponseCodes.NOT_FOUND:
            raise ResourceClientError('Pull from datastore failed in resource client! Requested Resource Type Not Found!')
        else:
            res_id = str(result.MessageResponseBody)
            
        # Check the result!
        new_repo = proc_ds1.workbench.get_repository(res_id)
            
        resource = yield new_repo.checkout('master')
            
        self.assertEqual(resource.identity, res_id)
        
        self.assertEqual(resource.object_type, resource_description.object_type)
        
        
        
        
        
        

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
