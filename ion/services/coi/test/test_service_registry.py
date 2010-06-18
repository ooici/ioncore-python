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

from ion.services.coi.service_registry import ServiceDesc, ServiceRegistryClient,\
 ServiceInstanceDesc
from ion.test.iontest import IonTestCase

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

        self.src = ServiceRegistryClient(proc=sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_service_reg(self):
        sd1 = ServiceDesc()
        sd1.svc_name='svc1'
        sd1.name = 'service'
        
        res1 = yield self.src.register_service(sd1)

        si1 = ServiceInstanceDesc()
        si1.xname='self.sup.id.full'
        si1.svc_name='svcinst1'
        si1.name = 'service instance'
        
        ri1 = yield self.src.register_service_instance(si1)

        ri2 = yield self.src.get_service_instance_name('svcinst1')
        self.assertEqual(ri2, 'self.sup.id.full')



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

    @defer.inlineCallbacks
    def test_service_reg(self):
        sd1 = ServiceDesc()
        sd1.name='svc1'
        
        c = ServiceRegistryClient(proc=self.sup)
        res1 = yield c.register_service(sd1)
            
        si1 = ServiceInstanceDesc()
        si1.xname=self.sup.id.full
        si1.svc_name='svc1'
        
        ri1 = yield c.register_service_instance(si1)

        ri2 = yield c.get_service_instance_name('svc1')
        self.assertEqual(ri2, self.sup.id.full)
