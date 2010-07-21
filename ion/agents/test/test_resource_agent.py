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
    import InstrumentAgentResourceDescription
class TestResourceAgent(IonTestCase):
    
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        # us some sort of agent...doesnt really matter what kind, does it
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

        # setup the resource registry client
        yield self.RAClient.set_resource_registry_client(str(self.rr_id))

        self.res_desc = InstrumentAgentResourceDescription()
        self.res_desc.name = "testAgent"
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        
    @defer.inlineCallbacks
    def test_reg_direct(self):
        id = str(uuid.uuid4())
        result = yield self.RRClient.register_resource(id,self.res_desc)
        self.assertEqual(result, id)
        
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
        
        