#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry_beta/test/test_resource_client.py
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
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.test.iontest import IonTestCase


class ResourceClientTest(IonTestCase):
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
        self.rc = ResourceClient(proc=sup)
        self.sup = sup

    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()
        
    @defer.inlineCallbacks
    def test_register_resource(self):
        
        type_id = yield self.rc.create_type_identifier(package='net.ooici.play', protofile='addressbook', cls='AddressLink')
        
        resource = yield self.rc.create_resource_instance(type_id, name='Test AddressLink Resource', description='A test resource')
        
        self.assertIsInstance(resource, ResourceInstance)
        self.assertEqual(resource.life_cycle_state, resource.NEW)
        self.assertEqual(resource.name, 'Test AddressLink Resource')
        self.assertEqual(resource.description, 'A test resource')
        
    @defer.inlineCallbacks
    def test_retrieve_resource(self):
            
        type_id = yield self.rc.create_type_identifier(package='net.ooici.play', protofile='addressbook', cls='AddressLink')
            
        resource = yield self.rc.create_resource_instance(type_id, name='Test AddressLink Resource', description='A test resource')
            
        res_id = resource.identity
            
        # Spawn a completely separate resource client and see if we can retrieve the resource...
        services = [
            {'name':'my_process','module':'ion.core.process.process','class':'Process'}]
        
        sup = yield self._spawn_processes(services)
        
        child_ps1 = yield self.sup.get_child_id('my_process')
        log.debug('Process ID:' + str(child_ps1))
        proc_ps1 = self._get_procinstance(child_ps1)
        
        my_rc = ResourceClient(proc=proc_ps1)
            
        my_resource = yield my_rc.retrieve_resource_instance(res_id)
            
        self.assertEqual(my_resource.name, 'Test AddressLink Resource')
        
    @defer.inlineCallbacks
    def test_read_resource(self):
            
        type_id = yield self.rc.create_type_identifier(package='net.ooici.play', protofile='addressbook', cls='AddressLink')
            
        resource = yield self.rc.create_resource_instance(type_id, name='Test AddressLink Resource', description='A test resource')
            
        obj = yield resource.read_resource()
        self.assertEqual(obj.GPBType, type_id)
            
        # Test read a version 
        obj = yield resource.read_resource(version='master')
        self.assertEqual(obj.GPBType, type_id)
            
        # Read an invalid version
        obj = yield resource.read_resource(version='masterXXX')
        self.assertEqual(obj.GPBType, None)
    
    @defer.inlineCallbacks
    def test_read_your_writes_resource(self):
            
        type_id = yield self.rc.create_type_identifier(package='net.ooici.play', protofile='addressbook', cls='AddressLink')
            
        resource = yield self.rc.create_resource_instance(type_id, name='Test AddressLink Resource', description='A test resource')
            
        obj = yield resource.read_resource()
        self.assertEqual(obj.GPBType, type_id)
        
        p=obj.person.add()
        p.id=5
        p.name='David'
        
        yield resource.write_resource('Testing write...')
        
        res_id = resource.identity
        
        # Spawn a completely separate resource client and see if we can retrieve the resource...
        services = [
            {'name':'my_process','module':'ion.core.process.process','class':'Process'}]
        
        sup = yield self._spawn_processes(services)
        
        child_ps1 = yield self.sup.get_child_id('my_process')
        log.debug('Process ID:' + str(child_ps1))
        proc_ps1 = self._get_procinstance(child_ps1)
        
        my_rc = ResourceClient(proc=proc_ps1)
            
        my_resource = yield my_rc.retrieve_resource_instance(res_id)
            
        self.assertEqual(my_resource.name, 'Test AddressLink Resource')
        
        my_obj = yield my_resource.read_resource()
        
        self.assertEqual(obj.person[0].name, 'David')
        
        
    @defer.inlineCallbacks
    def test_version_resource(self):
            
        type_id = yield self.rc.create_type_identifier(package='net.ooici.play', protofile='addressbook', cls='AddressLink')
            
        resource = yield self.rc.create_resource_instance(type_id, name='Test AddressLink Resource', description='A test resource')
            
        obj = yield resource.read_resource()
        self.assertEqual(obj.GPBType, type_id)
        
        p=obj.person.add()
        p.id=5
        p.name='David'
        
        yield resource.write_resource('Testing write...')
        
        res_id = resource.identity
        
        # Spawn a completely separate resource client and see if we can retrieve the resource...
        services = [
            {'name':'my_process','module':'ion.core.process.process','class':'Process'}]
        
        sup = yield self._spawn_processes(services)
        
        child_ps1 = yield self.sup.get_child_id('my_process')
        log.debug('Process ID:' + str(child_ps1))
        proc_ps1 = self._get_procinstance(child_ps1)
        
        my_rc = ResourceClient(proc=proc_ps1)
            
        my_resource = yield my_rc.retrieve_resource_instance(res_id)
            
        self.assertEqual(my_resource.name, 'Test AddressLink Resource')
        
        my_obj = yield my_resource.read_resource()
        
        self.assertEqual(obj.person[0].name, 'David')
        