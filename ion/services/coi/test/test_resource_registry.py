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
        res = dataobject.InformationResource.create_new_resource()
        res = yield self.rrc.register_resource_definition(res)
        
        res = yield self.rrc.set_resource_lcstate_commissioned(res)
        
        ref = res.reference()
        
        res2 = yield self.rrc.get_resource_definition(ref)
        
        self.assertEqual(res2,res)
        print res
        
    def test_describe_resource(self):
        # put in a bogus resource for now...
        res = coi_resource_descriptions.ResourceDescription.create_new_resource()
        
        res.describe_resource(res)
        print 'res:',res



class ResourceRegistryCoreServiceTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.sup = yield self._start_core_services()
        #logging.info('self.sup.proc_state'+str(self.sup.proc_state))
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


