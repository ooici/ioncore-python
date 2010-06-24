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

from ion.services.coi.resource_registry import ResourceRegistryClient
from ion.test.iontest import IonTestCase

import uuid

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
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_resource_reg(self):

        rd2 = registry.Generic()
        rd2.name = 'David'
        
        rid = yield self.rrc.register_resource(str(uuid.uuid4()),rd2) 
        logging.info('Resource registered with id '+str(rid))

        rd3 = yield self.rrc.get_resource(rid)
        logging.info('Resource desc:\n '+str(rd3))
        self.assertEqual(rd3,rd2)

    @defer.inlineCallbacks
    def test_resource_state(self):

        rd2 = registry.Generic()
        rd2.name = 'David'
        
        rid = yield self.rrc.register_resource(str(uuid.uuid4()),rd2) 
        logging.info('Resource registered with id '+str(rid))

        rd3 = yield self.rrc.get_resource(rid)
        logging.info('Resource desc:\n '+str(rd3))
        self.assertEqual(rd3,rd2)
        
        # Change the state and make sure the result is correct
        yield self.rrc.set_lcstate(rid,registry.LCStates.active)
        rd4 = yield self.rrc.get_resource(rid)
        logging.info('Resource desc:\n '+str(rd4))
        self.assertNotEqual(rd3,rd4)

        # Make sure set_lcstate returns true for a valid id
        success = yield self.rrc.set_lcstate_new(rid)
        self.assertEqual(success,True)
        
        # Try to set the state of a resource which does not exist
        success = yield self.rrc.set_lcstate_new('dklhshkaviohe23290')
        self.assertEqual(success,False)
        
        
        

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
    def test_resource_reg(self):

        rd2 = registry.Generic()
        rd2.name = 'David'
        
        reg = ResourceRegistryClient(proc=self.sup)
        rid = yield reg.register_resource(str(uuid.uuid4()),rd2) 
        logging.info('Resource registered with id '+str(rid))

        rd3 = yield reg.get_resource(rid)
        logging.info('Resource desc:\n '+str(rd3))
        self.assertEqual(rd3,rd2)

        rd4 = yield reg.get_resource('NONE')
        logging.info('rd4'+str(rd4))
        self.assertFalse(rd4,'resource desc not None')
