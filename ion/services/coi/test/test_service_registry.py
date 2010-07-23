#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging
logging = logging.getLogger(__name__)
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
        
        play_desc = yield self.src.register_service_defintion(hello_service.HelloService)
        
        ref = play_desc.reference()
        
        svc_desc = yield self.src.get_service_definition(ref)
        
        print svc_desc

    #@defer.inlineCallbacks
    def test_service_instance_reg(self):
        """
        """
        raise unittest.SkipTest('Not implimented yet!')
        
        #services = [
        #    {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
        #]

        #sup = yield self._spawn_processes(services)
        
        #print type(sup)
        #print sup.__dict__
        #print sup.child_procs[0]
        
        #bs_svc = hello_service.HelloService(self.sup.reciever)
        
        #print 'HELLO'
        
        #@Note Can't get at the service instance Object Need to pass supervisor instead of object instance...
        
        #play_desc = yield self.src.register_service_defintion(hello_service.HelloService)
        #ref = play_desc.reference()
        #svc_desc = yield self.src.get_service_definition(ref)
        #print svc_desc


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