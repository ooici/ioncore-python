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
from ion.data import dataobject

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
        
        
        
        
    @defer.inlineCallbacks
    def test_find_resource(self):
        
        class test_resource(registry.ResourceDescription):
            ival = dataobject.TypedAttribute(int)

        rd1 = test_resource()
        rd1.name = 'David'
        rd1.ival = 1000
                
        rd1.set_lifecyclestate(registry.LCStates.active)
                
        rd2 = test_resource()
        rd2.name = 'Dorian'
        
        rd3 = registry.Generic()
        rd3.name = 'John'
        
#        rid1 = yield self.rrc.register_resource(str(uuid.uuid4()),rd1)
#        rid2 = yield self.rrc.register_resource(str(uuid.uuid4()),rd2)
#       rid3 = yield self.rrc.register_resource(str(uuid.uuid4()),rd3)
        
        rid1 = yield self.rrc.register_resource('foo',rd1)
        rid2 = yield self.rrc.register_resource('poo',rd2)
        rid3 = yield self.rrc.register_resource(str(uuid.uuid4()),rd3)
        
        # Just to see what happens modify John
        rd3.name = 'John Graybeal'
        rid3 = yield self.rrc.register_resource(rid3,rd3)
        
        test = yield self.rrc.get_resource(rid3)
        self.assertEqual(test,rd3)
        
        logging.info('**find a resource**')
        # Test for a valid attribute
        resources = yield self.rrc.find_resources({'name':'David','ival':1000})
        #print resources[0]
        self.assertIn(rd1,resources)
        self.assertNotIn(rd2, resources)
        self.assertNotIn(rd3, resources)

        
        resources = yield self.rrc.find_resources({'name':'David','ival':2000})
        #print resources[0]
        self.assertEqual(resources,[])
        
        
        # test for an invalid attribute
        resources = yield self.rrc.find_resources({'names':'David'})
        self.assertEqual(resources,[])
        
        
        
        resources = yield self.rrc.find_resources({'name':'D'})
        self.assertIn(rd1, resources)
        self.assertIn(rd2, resources)
        self.assertNotIn(rd3, resources)

        resources = yield self.rrc.find_resources({'name':None})
        self.assertIn(rd1, resources)
        self.assertIn(rd2, resources)
        self.assertIn(rd3, resources)


        resources = yield self.rrc.find_resources({'lifecycle':None})
        self.assertIn(rd1, resources)
        self.assertIn(rd2, resources)
        self.assertIn(rd3, resources)
                
        # Finding lifecycle is bogus for now!
        # you can not transmitt a LCState object in a dict - must use the string
        resources = yield self.rrc.find_resources({'lifecycle':'active'})
        self.assertIn(rd1, resources)
        self.assertNotIn(rd2, resources)
        self.assertNotIn(rd3, resources)
        


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
