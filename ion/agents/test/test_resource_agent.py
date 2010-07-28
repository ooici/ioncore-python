#!/usr/bin/env python

"""
@file ion/agents/test/test_resource_agent.py
@author Steve Foley
"""
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from ion.data.dataobject import LCState

from ion.test.iontest import IonTestCase
from ion.agents.resource_agent import ResourceAgentClient
from ion.services.coi.agent_registry import AgentRegistryClient
from ion.resources.ipaa_resource_descriptions \
    import InstrumentAgentResourceDescription, InstrumentAgentResourceInstance
from ion.data.dataobject import TypedAttribute, LCStates

class TestResourceAgent(IonTestCase):
    
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        # use some sort of agent...doesnt really matter what kind, does it
        processes = [
            {'name':'testAgent',
             'module':'ion.agents.instrumentagents.SBE49_IA',
             'class':'SBE49InstrumentAgent'},
            {'name':'agent_registry',
             'module':'ion.services.coi.agent_registry',
             'class':'AgentRegistryService'}
        ]
        self.sup = yield self._spawn_processes(processes)
        self.svc_id = self.sup.get_child_id('testAgent')
        self.rr_id = self.sup.get_child_id('agent_registry')
        
        # Start a client (for the RPC)
        self.RAClient = ResourceAgentClient(proc=self.sup, target=self.svc_id)
        # RR Client is ,,,,,desired only for testing
        self.reg_client = AgentRegistryClient(proc=self.sup, target=self.rr_id)
        yield self.reg_client.clear_registry()
        
        # setup the registry client
        yield self.RAClient.set_registry_client(str(self.rr_id))

        # need any non-generic resource...use an instrument agent one for now
        self.res_desc = InstrumentAgentResourceDescription.create_new_resource()
        self.res_desc.version = '1.23'
        self.res_desc.name = 'I am a test IA resource description'
        
        self.res_inst = InstrumentAgentResourceInstance.create_new_resource()
        self.res_inst.driver_process_id = 'something_for_now.1'
        self.res_inst.name = 'I am an instantiation of a test IA resource'
        
        logging.debug("*** res_desc: %s, res_inst: %s", self.res_desc, self.res_inst)
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        
    @defer.inlineCallbacks
    def test_reg_setstate_direct(self):
        registered_agent_desc = \
           yield self.reg_client.register_agent_definition(self.res_desc)
        logging.debug("*** register_def result: %s", registered_agent_desc)
        # Not committed, so not equal, but parts should be
        self.assertNotEqual(registered_agent_desc, self.res_desc)
        self.assertEqual(registered_agent_desc.RegistryIdentity,
                         self.res_desc.RegistryIdentity)
        self.assertEqual(registered_agent_desc.version,
                         self.res_desc.version)
        
        registered_agent_instance = \
            yield self.reg_client.register_agent_instance(self.res_inst)
        logging.debug("*** register_inst result: %s",
                      registered_agent_instance)        
        # Not committed, so not equal, but parts should be
        self.assertNotEqual(registered_agent_instance, self.res_inst)
        self.assertEqual(registered_agent_instance.RegistryIdentity,
                         self.res_inst.RegistryIdentity)
                
        recv_agent_desc = \
            yield self.reg_client.get_agent_definition(self.res_desc)
        logging.debug("*** get_def result: %s", recv_agent_desc)        
        self.assertEqual(recv_agent_desc, registered_agent_desc)
        self.assertEqual(recv_agent_desc.RegistryIdentity,
                         self.res_desc.RegistryIdentity)
        # Not committed, so not equal
        self.assertNotEqual(recv_agent_desc, self.res_desc)
        
        recv_agent_inst = yield self.reg_client.get_agent_instance(self.res_inst)
        logging.debug("*** get_inst result: %s", recv_agent_inst)        
        self.assertEqual(recv_agent_inst, registered_agent_instance)
        # Not commiteed, so not equal, but parts should be
        self.assertNotEqual(recv_agent_inst, self.res_inst)
        self.assertEqual(recv_agent_inst.RegistryIdentity,
                         self.res_inst.RegistryIdentity)
        
    @defer.inlineCallbacks
    def test_registration(self):
        # initial registration of instance
        reg_id = yield self.RAClient.register_resource(self.res_inst)
        result = yield self.RAClient.get_resource_instance()
        logging.debug("*** res_desc: %s, result: %s", self.res_inst, result)
        self.assertNotEqual(result, None)
        self.assertNotEqual(result, self.res_inst)
        self.assertNotEqual(result.RegistryCommit,
                            self.res_inst.RegistryCommit)
        self.assertEqual(self.res_inst.RegistryIdentity,
                         result.RegistryIdentity)
     
        # Verify the reference is the same   
        result = yield self.RAClient.get_resource_ref()
        self.assertEqual(result, reg_id)
         
        # test update/repeat reg if a different instance
        new_res_inst = yield self.RAClient.get_resource_instance()
        new_res_inst.name = "UPDATED TestAgentInstance"
        new_res_inst.driver_process_id = 'something_else.1'

        result = yield self.RAClient.register_resource(new_res_inst)
        self.assertEqual(reg_id, result)
        result = yield self.RAClient.get_resource_instance()
        self.assertEqual(new_res_inst.name, result.name)
        self.assertEqual(new_res_inst.driver_process_id,
                         result.driver_process_id)
    
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
        
        