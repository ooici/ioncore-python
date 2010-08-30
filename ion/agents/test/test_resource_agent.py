#!/usr/bin/env python

"""
@file ion/agents/test/test_resource_agent.py
@author Steve Foley
"""
import logging
log = logging.getLogger(__name__)
from twisted.internet import defer
from ion.data.dataobject import LCState

from ion.test.iontest import IonTestCase
from ion.agents.resource_agent import ResourceAgentClient
from ion.services.coi.agent_registry import AgentRegistryClient
from ion.resources.ipaa_resource_descriptions \
    import InstrumentAgentResourceDescription, InstrumentAgentResourceInstance

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
        self.reg_client = AgentRegistryClient(proc=self.sup)
        yield self.reg_client.clear_registry()

        # need any non-generic resource...use an instrument agent one for now
        self.res_desc = \
            InstrumentAgentResourceDescription.create_new_resource()
        #self.res_desc.version = '1.23'
        self.res_desc.name = 'I am a test IA resource description'

        self.res_inst = InstrumentAgentResourceInstance.create_new_resource()
        self.res_inst.driver_process_id = 'something_for_now.1'
        self.res_inst.name = 'I am an instantiation of a test IA resource'

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_reg_direct(self):
        registered_agent_desc = \
           yield self.reg_client.register_agent_definition(self.res_desc)
        # Not committed, so not equal, but parts should be
        self.assertNotEqual(registered_agent_desc, self.res_desc)
        self.assertEqual(registered_agent_desc.RegistryIdentity,
                         self.res_desc.RegistryIdentity)

        registered_agent_instance = \
            yield self.reg_client.register_agent_instance(self.res_inst)
        # Not committed, so not equal, but parts should be
        self.assertNotEqual(registered_agent_instance, self.res_inst)
        self.assertEqual(registered_agent_instance.RegistryIdentity,
                         self.res_inst.RegistryIdentity)

        recv_agent_desc = \
            yield self.reg_client.get_agent_definition(self.res_desc)
        self.assertEqual(recv_agent_desc, registered_agent_desc)
        self.assertEqual(recv_agent_desc.RegistryIdentity,
                         self.res_desc.RegistryIdentity)
        # Not committed, so not equal
        self.assertNotEqual(recv_agent_desc, self.res_desc)

        recv_agent_inst = \
            yield self.reg_client.get_agent_instance(self.res_inst)
        self.assertEqual(recv_agent_inst, registered_agent_instance)
        # Not commiteed, so not equal, but parts should be
        self.assertNotEqual(recv_agent_inst, self.res_inst)
        self.assertEqual(recv_agent_inst.RegistryIdentity,
                         self.res_inst.RegistryIdentity)

    @defer.inlineCallbacks
    def test_registration_with_definitions(self):
        reg_id = yield self.RAClient.register_resource(self.res_inst)
        orig_result = yield self.RAClient.get_resource_instance()
        self.assertNotEqual(orig_result, None)
        self.assertNotEqual(orig_result, self.res_inst)
        self.assertNotEqual(orig_result.RegistryCommit,
                            self.res_inst.RegistryCommit)
        self.assertEqual(reg_id.RegistryCommit, '')
        self.assertNotEqual(orig_result.RegistryCommit, reg_id.RegistryCommit)
        self.assertEqual(reg_id.RegistryIdentity, orig_result.RegistryIdentity)

        # Verify the reference is the same
        result = yield self.RAClient.get_resource_ref()
        self.assertEqual(result, reg_id)

        # test update/repeat reg if a different instance
        new_res_inst = yield self.RAClient.get_resource_instance()
        new_res_inst.name = "REPLACED TestAgentInstance"
        new_res_inst.driver_process_id = 'something_else.1'

        # Even pulling it, modifiying it, then re-registering makes a new one
        new_result = yield self.RAClient.get_resource_instance()
        new_result.name = "UPDATED TestAgentInstance"
        new_result.driver_process_id = 'something_else.2'
        new_reg_ref = yield self.RAClient.register_resource(new_res_inst)
        self.assertNotEqual(reg_id, new_reg_ref)
        new_result2 = yield self.RAClient.get_resource_instance()
        self.assertNotEqual(new_result2.RegistryIdentity,
                            orig_result.RegistryIdentity)
        self.assertNotEqual(new_result2.RegistryCommit,
                            orig_result.RegistryCommit)
        self.assertNotEqual(new_result2.name, orig_result.name)
        self.assertEqual(new_result2.name, new_res_inst.name)
        self.assertEqual(new_result2.driver_process_id,
                         new_res_inst.driver_process_id)
        self.assertNotEqual(new_result2.name, new_result.name)

    @defer.inlineCallbacks
    def test_agent_self_registration(self):
        reg_id = yield self.RAClient.register_resource()
        result = yield self.RAClient.get_resource_instance()
        self.assertNotEqual(result, None)
        self.assertEqual(reg_id.RegistryCommit, '')
        self.assertNotEqual(result.RegistryCommit, reg_id.RegistryCommit)
        self.assertEqual(reg_id.RegistryIdentity, result.RegistryIdentity)

        # Verify the reference is the same
        result = yield self.RAClient.get_resource_ref()
        self.assertEqual(result, reg_id)

        # Make A new agent to verify we have 2
        processes = [{'name':'testAgent2',
                      'module':'ion.agents.instrumentagents.SBE49_IA',
                      'class':'SBE49InstrumentAgent'}]

        sup2 = yield self._spawn_processes(processes)
        svc_id2 = self.sup.get_child_id('testAgent2')

        # Start a client (for the RPC)
        RAClient2 = ResourceAgentClient(proc=sup2, target=svc_id2)

        # test update/repeat reg if a different instance
        yield RAClient2.register_resource()
        refinst2 = yield RAClient2.get_resource_instance()
        refinst1 = yield self.RAClient.get_resource_ref()

        self.assertNotEqual(refinst1.RegistryCommit, refinst2.RegistryCommit)
        self.assertNotEqual(refinst1.RegistryIdentity,
                            refinst2.RegistryIdentity)
        self.assertEqual(refinst1.RegistryCommit, result.RegistryCommit)
        self.assertEqual(refinst1.RegistryIdentity, result.RegistryIdentity)


    @defer.inlineCallbacks
    def test_lifecycle(self):
        registered_res = yield self.RAClient.register_resource(self.res_inst)
        self.assertNotEqual(registered_res, None)
        result = yield self.RAClient.get_resource_ref()
        self.assertEqual(result, registered_res)

        active_state = LCState('active')
        inactive_state = LCState('inactive')

        result = yield self.RAClient.set_lifecycle_state(active_state)
        self.assertEqual(result, active_state)
        result = yield self.RAClient.get_lifecycle_state()
        self.assertEqual(result, active_state)

        result = yield self.RAClient.set_lifecycle_state(inactive_state)
        ref = yield self.RAClient.get_resource_ref()
        self.assertNotEqual(ref, None)
        self.assertEqual(ref, registered_res)
        result = yield self.RAClient.get_lifecycle_state()
        self.assertEqual(result, inactive_state)
