#!/usr/bin/env python

"""
@file ion/services/coi/agent_registry.py
@author David Stuebe
@brief service for registering agents
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect
from magnet.spawnable import Receiver

from ion.data import dataobject
from ion.data.datastore import registry
from ion.data import store

import inspect

from ion.core import ioninit
from ion.core import base_process
from ion.core.base_process import ProtocolFactory, BaseProcess
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

from ion.resources import coi_resource_descriptions

CONF = ioninit.config(__name__)


class AgentRegistryService(registry.BaseRegistryService):
    """
    Agent registry service interface
    The Resource Registry Service uses an IStore interface to a backend Key
    Value Store to store to track version controlled objects. The store will
    share a name space and share objects depending on configuration when it is
    created. The resource are retrieved as complete objects from the store. The
    built-in encode method is used to store and transmit them using the COI
    messaging.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='agent_registry', version='0.1.0', dependencies=[])

    
    op_clear_registry = registry.BaseRegistryService.base_clear_registry
    
    
    op_register_agent_instance = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register an agent instance with the registry.
    """
    op_get_agent_instance = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get an agent instance.
    """
    
    op_register_agent_definition = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update an agent definition with the registry.
    """
    op_get_agent_definition = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get an agent definition.
    """
    
    op_set_agent_instance_lcstate = registry.BaseRegistryService.base_set_resource_lcstate
    """
    Service operation: Set an agent instance life cycle state
    """
    
    op_find_agent_definition_from_agent = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the registered definition of an agent from its class
    """
    op_find_agent_definitions_from_description = registry.BaseRegistryService.base_find_resource
    """
    Service operation: find all registered agents which match the attributes of description
    """
    
    op_find_registered_agent_instance_from_instance = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the registered instance of an agent from its self
    """
    op_find_registered_agent_instance_from_description = registry.BaseRegistryService.base_find_resource
    """
    Service operation: find all registered instances of agents which match the attributes of description
    """
    
class agentRegistryClient(registry.BaseRegistryClient, registry.LCStateMixin):
    """
    Class for the client accessing the agent registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "agent_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_registry')

    def register_container_agents(self):
        """
        This method is called when the container is started to inspect the
        agents directory of lca arch and register descriptions for all
        agents.
        """

    @defer.inlineCallbacks
    def register_agent_instance(self,agent_instance,owner):
        """
        Client method to Register an agent instance
        An instance is a reference to an owner, a description and the agent
        @Note this need architectural clarification
        """
        agent_instance_description = yield self.describe_instance(agent_instance,owner)
        
        found_rid = yield self.find_registered_agent_instance_from_description(agent_instance,regex=False,ignore_defaults=True)
        if found_rid:
            # if it is already there, return it.
            defer.returnValue(found_rid)
        else:
            # Give it a new Unique ID and put it in the registry
            agent_instance_description.create_new_reference()
            agent_instance_description = self.base_register_agent(agent_instance_description, 'register_agent_instance')    
            defer.returnValue(agent_instance_description)
        
    @defer.inlineCallbacks
    def describe_instance(self,agent_instance,owner):
        """
        @Brief Extract metadata from a agent instance to store in the agent
        registry
        @Param agent is an instance of a class which inherits from agent
        """
        assert isinstance(agent_instance, dataobject.Resource)
        assert isinstance(owner, coi_agent_descriptions.IdentityResource)

        agent_instance_description = coi_resource_descriptions.AgentInstance()
        agent_instance_description.name = agent_instance.name # ?

        #Get the registry description for this agent
        agent_description = yield self.register_agent_definition(agent_instance.__class__)
        agent_instance_description.description = agent_description.reference(head=True)

        agent_instance_description.owner = owner.reference(head=True)
        agent_instance_description.agent = agent_instance.reference(head=True)
        
        # Set the life cycle state to developed?
        agent_instance_description.set_lifecyclestate(dataobject.LCStates.developed)
        
        defer.returnValue(agent_instance_description)
        
    def get_agent_instance(self,agent_reference):
        """
        Get a agent instance
        """
        return self.base_get_agent(agent_reference,'get_agent_instance')
        
        
    @defer.inlineCallbacks
    def register_agent_definition(self,agent_class):
        """
        Client method to register the Definition of a agent Type
        """
        agent_description = yield self.describe_agent(agent_class)

        found_rd = yield self.find_agent_definitions_from_description(agent_description,regex=False,ignore_defaults=True)
        if found_rd:
            assert len(found_rd) == 1
            defer.returnValue(found_rd[0])
        else:
            agent_description.create_new_reference()
            agent_description = yield self.base_register_agent(agent_description, 'register_agent_definition')
            defer.returnValue(agent_description)
        
    @defer.inlineCallbacks
    def describe_agent(self,agent_class):
        """
        @Brief Extract metadata from a agent subclass to store in the agent
        registry
        @Param agent is a class which inherits from agent
        """
        assert issubclass(agent_class, dataobject.Resource)
        
        agent_description = coi_agent_descriptions.agentDescription()
        agent_description.name = agent_class.__name__
        agent_description.description = inspect.getdoc(agent_class)
        
        # Get all the typed attributes of the agent
        for name, att in agent_class.get_typedattributes().items():
            attdesc = coi_resource_descriptions.AttributeDescription()
            attdesc.name = name
            attdesc.type = str(att.type)
            attdesc.default = str(att.default)
            agent_description.atts.append(attdesc)    
        
        # Get the reference to the agent it inherits from
        agent_description.inherits_from = yield self.get_agent_bases_by_reference(agent_class)
                
        # Set the type based on inheritance?
        if issubclass(agent_class, dataobject.InformationResource):
            agent_description.type = coi_resource_descriptions.OOIResourceTypes.information
        elif issubclass(agent_class, dataobject.StatefulResource):
            agent_description.type = coi_resource_descriptions.OOIResourceTypes.stateful
        else:
            agent_description.type = coi_resource_descriptions.OOIResourceTypes.unassigned
        
        # Update the lifecycle state and return
        agent_description.set_lifecyclestate(dataobject.LCStates.developed)
        defer.returnValue(agent_description)
        
        
        

    @defer.inlineCallbacks
    def get_agent_bases_by_reference(self,agent_class):
        
        bases = self.get_parent_agent_classes(agent_class)
        
        bases_refs=[]
        for base in bases:
            parent_agent_description = yield self.find_agent_definition_from_agent(base)
            if not parent_agent_description:
                parent_agent_description = yield self.register_agent_definition(base)
            
            bases_refs.append(parent_agent_description.reference())

        defer.returnValue(bases_refs)
        

    def get_parent_agent_classes(self,agent_class):
        assert issubclass(agent_class, dataobject.Resource)
        # Ignore multiple inheritence for now!
        bases=[]
        for base in agent_class.__bases__:
            if issubclass(base, dataobject.Resource):
                bases.append(base)
        return bases
        

    def get_agent_definition(self,agent_reference):
        """
        Get a agent definition
        """
        return self.base_get_agent(agent_reference,'get_agent_definition')

        
    def set_agent_lcstate(self, agent_reference, lcstate):
        return self.base_set_resource_lcstate(agent_reference, lcstate, 'set_agent_lcstate')

    @defer.inlineCallbacks
    def find_agent_definition_from_agent(self, agent_class):
        """
        @Brief find the registered definition of a resoruce
        """
        agent_description = yield self.describe_agent(agent_class)
        description_list = yield self.base_find_resource(agent_description,'find_agent_definition_from_agent',regex=False,ignore_defaults=True)
        # Find returns a list but only one agent should match!
        if description_list:
            assert len(description_list) == 1
            defer.returnValue(description_list[0])
        else:
            defer.returnValue(None)
            
    def find_agent_definitions_from_description(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered agents which match the attributes of description
        """
        return self.base_find_resource(description,'find_agent_definitions_from_description',regex,ignore_defaults,attnames)
        
    @defer.inlineCallbacks
    def find_registered_agent_instance_from_instance(self, agent_instance, owner):
        """
        @Brief Find the registered instance of a agent
        """
        agent_instance_description = yield self.describe_instance(agent_instance,owner)
        agent_list = yield self.base_find_resource(agent_instance_description,'find_registered_agent_instance_from_instance',regex=False,ignore_defaults=True)
        # Find returns a list but only one agent should match!
        if agent_list:
            assert len(agent_list) == 1
            defer.returnValue(agent_list[0])
        else:
            defer.returnValue(None)

    def find_registered_agent_instance_from_description(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered agents which match the attributes of description
        """
        return self.base_find_resource(description,'find_registered_agent_instance_from_description',regex,ignore_defaults,attnames)

# Spawn of the process using the module name
factory = ProtocolFactory(AgentRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""
