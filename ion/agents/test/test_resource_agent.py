#!/usr/bin/env python

"""
@file ion/agents/test/test_resource_agent.py
@author Steve Foley
"""

import uuid
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from ion.data.dataobject import LCState

from ion.test.iontest import IonTestCase
from ion.agents.resource_agent import ResourceAgentClient
from ion.services.coi.resource_registry import ResourceRegistryClient
from ion.resources.ipaa_resource_descriptions \
    import InstrumentAgentResourceDescription, InstrumentAgentResourceInstance
from ion.data.dataobject import InformationResource, TypedAttribute

class TestResourceAgent(IonTestCase):
    
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        # use some sort of agent...doesnt really matter what kind, does it
        processes = [
            {'name':'testAgent',
             'module':'ion.agents.instrumentagents.SBE49_IA',
             'class':'SBE49InstrumentAgent'},
            {'name':'resource_registry',
             'module':'ion.services.coi.resource_registry',
             'class':'ResourceRegistryService'}
        ]
        self.sup = yield self._spawn_processes(processes)
        self.svc_id = self.sup.get_child_id('testAgent')
        self.rr_id = self.sup.get_child_id('resource_registry')
        
        # Start a client (for the RPC)
        self.RAClient = ResourceAgentClient(proc=self.sup, target=self.svc_id)
        # RR Client is ,,,,,desired only for testing
        self.RRClient = ResourceRegistryClient(proc=self.sup, target=self.rr_id)
        yield self.RRClient.clear_registry()
        
        # setup the resource registry client
        yield self.RAClient.set_resource_registry_client(str(self.rr_id))

        """
        res_to_describe = dataobject.InformationResource.create_new_resource()
        res_description = yield self.rrc.register_resource_definition(res_to_describe)
        ref = yield self.rrc.set_resource_lcstate_commissioned(res_description)
        commissioned_description = yield self.rrc.get_resource_definition(ref)
        """
        
        """
        I want to:
        - create a resource description with specific fields (where do I inherit from?)
        - create an instance of that description with some data specific to my instance
        - Link the instance to the description
        - register the resource description and instance
        - Change the lifecycle state of the resource, get it to confirm
        
        """
        # need any non-generic resource...use an instrument agent one for now
        self.res_desc = InstrumentAgentResourceDescription.create_new_resource()
        self.resource_desc.driver_class = TypedAttribute( \
            "ion.agent.instrumentagents.SBE49_instrument_driver.SBE49InstrumentDriver")
        
        self.res_inst = InstrumentAgentResourceInstance.create_new_resource()
        self.res_inst.driver_process_id = "something_for_now.1"
        
        logging.debug("*** resource: %s", self.resource)
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        
    @defer.inlineCallbacks
    def test_reg_setstate_direct(self):
        result = yield self.RRClient.register_resource_definition(self.res_desc)
        logging.debug("*** register_def result: %s", result)
        result = yield self.RRClient.register_resource_instance(self.res_inst)
        logging.debug("*** register_inst result: %s", result)        
        result = yield self.RRClient.get_resource_definition()
        logging.debug("*** get_def result: %s", result)        
        result = yield self.RRClient.get_resource_instance()
        logging.debug("*** get_inst result: %s", result)        

        # assert some stuff
        result = yield self.RRClient.set_resource_lcstate(self.res_inst, LCState.active)
        result = yield self.RRClient.get_resource_instance()
        # get the state out of this somehow and assert stuff
        result = yield self.RRClient.set_resource_lcstate(self.res_inst, LCState.active)
        result = yield self.RRClient.set_resource_lcstate(self.res_inst, LCState.developed)
        result = yield self.RRClient.set_resource_lcstate(self.res_inst, LCState.active)

        
    @defer.inlineCallbacks
    def test_registration(self):
        # initial registration
        reg_id = yield self.RAClient.register_resource(self.res_desc)
        result = yield self.RAClient.get_resouce_description(reg_id)
        self.assertEqual(self.res_desc, result)
     
        # test update/repeat registration
        new_res_desc = InstrumentAgentResourceDescription()
        new_res_desc.name = "newTestAgent"
        result = yield self.RAClient.register_resource(new_res_desc)
        self.assertEqual(reg_id, result)
        result = yield self.RAClient.get_resouce_description(reg_id)
        self.assertEqual(new_res_desc, result)
    
    @defer.inlineCallbacks
    def test_lifecycle(self):
        reg_id = yield self.RAClient.register_resource(self.res_desc)
        
        active_state = LCState('active')
        inactive_state = LCState('inactive')
        result = yield self.RAClient.set_lifecycle_state(active_state)
        self.assert_(reg_id > 0)
        result = yield self.RAClient.get_resource_id()
        self.assertEqual(result, reg_id)
        result = yield self.RAClient.get_lifecycle_state()
        self.assert_(result, active_state)
        
        reg_id = yield self.RAClient.set_lifecycle_state(inactive_state)
        self.assert_(reg_id > 0)
        result = yield self.RAClient.get_resource_id()
        self.assertEqual(result, reg_id)
        result = yield self.RAClient.get_lifecycle_state()
        self.assert_(result, inactive_state)
        
        