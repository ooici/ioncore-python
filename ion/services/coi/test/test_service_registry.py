#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging
log = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.services.coi.service_registry import ServiceRegistryClient 
from ion.test.iontest import IonTestCase

from ion.play import hello_service

class ServiceRegistryTest(IonTestCase):
    """
    Testing client classes of service registry
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._start_core_services()
        services = [
            {'name':'serviceregistry1','module':'ion.services.coi.service_registry','class':'ServiceRegistryService'}]
        sup = yield self._spawn_processes(services)

        self.sup = sup
        self.src = ServiceRegistryClient(proc=sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_service_reg(self):
        
        # Register a service class object
        play_desc = yield self.src.register_service_definition(hello_service.HelloService)
        
        # create a reference to the description in the registry
        ref = play_desc.reference()
        
        # get the description back from its reference
        svc_desc = yield self.src.get_service_definition(ref)
        
        #print svc_desc

    @defer.inlineCallbacks
    def test_service_instance_reg(self):
        """
        """        
        services = [
            {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
        ]
        # Create a process supervisor class
        sup = yield self._spawn_processes(services, sup=self.sup)
        
        # Register the service instance from the supervisors child_procs - Agument is a ProcessDesc object.
        # See base process!
        play_desc = yield self.src.register_service_instance(sup.child_procs[1])
        #print play_desc
        


class ServiceRegistryCoreServiceTest(IonTestCase):
    """
    Testing client classes of service registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.sup = yield self._start_core_services()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    #@defer.inlineCallbacks
    def test_service_reg(self):
        raise unittest.SkipTest('Not implimented yet!')
        # Not sure what the point is here?
        # Lets get it integrated with base process!
        
        
        