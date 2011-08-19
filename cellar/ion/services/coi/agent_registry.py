#!/usr/bin/env python

"""
@file ion/services/coi/agent_registry.py
@author Michael Meisinger
@author David Stuebe
@brief service for registering agent (types and instances).
"""
'''
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import inspect

from ion.core.process.process import Process
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.data.datastore import registry
from ion.data import dataobject

from ion.resources import coi_resource_descriptions


from ion.core import ioninit
CONF = ioninit.config(__name__)


class AgentRegistryService(registry.BaseRegistryService):
    """
    Agent registry service interface
    @todo a agent is a resource and should also be living in the resource registry
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='agent_registry', version='0.1.0', dependencies=[])

    op_clear_registry = registry.BaseRegistryService.base_clear_registry

    op_register_agent_definition = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register a agent definition with the registry.
    """
    op_get_agent_definition = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a agent definition.
    """

    op_register_agent_instance = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register a agent instance with the registry.
    """
    op_get_agent_instance = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a agent instance.
    """

    op_set_agent_lcstate = registry.BaseRegistryService.base_set_resource_lcstate
    """
    Service operation: Set a agent life cycle state
    """

    op_find_registered_agent_definition_from_agent = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the definition of a agent
    """
    op_find_registered_agent_definition_from_description = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find agent definitions which meet a description
    """

    op_find_registered_agent_instance_from_agent = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the registered instance that matches the agent instance
    """
    op_find_registered_agent_instance_from_description = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find all the registered agent instances which match a description
    """
# Spawn of the process using the module name
factory = ProcessFactory(AgentRegistryService)


class AgentRegistryClient(registry.BaseRegistryClient):
    """
    Client class for accessing the agent registry. This is most important for
    finding and accessing any other agents. This client knows how to find the
    agent registry - what does that mean, don't all clients have targetname
    assigned?
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "agent_registry"
        ServiceClient.__init__(self, proc, **kwargs)

    def clear_registry(self):
        return self.base_clear_registry('clear_registry')

    def register_container_agents(self):
        """
        This method is called when the container is started to inspect the
        agents of lca arch and register descriptions for each agent.
        """

    @defer.inlineCallbacks
    def register_agent_definition(self,agent):
        """
        Client method to register the Definition of a Agent Class
        """
        if isinstance(agent, coi_resource_descriptions.AgentDescription):
            agent_description = agent
            assert agent_description.RegistryIdentity, 'Agent Description must have a registry Identity'

        else:
            agent_class = agent
            # Build a new description of the agent
            agent_description = self.describe_agent(agent_class)

            found_sd = yield self.find_registered_agent_definition_from_description(agent_description)

            if found_sd:
                assert len(found_sd) == 1
                defer.returnValue(found_sd[0])
            else:
                agent_description.create_new_reference()

        agent_description = yield self.base_register_resource('register_agent_definition', agent_description)
        defer.returnValue(agent_description)


    def describe_agent(self,agent_class):

        assert issubclass(agent_class, Process)

        # Do not make a new resource idenity - this is a generic method which
        # is also used to look for an existing description
        agent_description = coi_resource_descriptions.AgentDescription()

        #agent_description.name = agent_class.declare['name']
        #agent_description.version = agent_class.declare['version']

        agent_description.class_name = agent_class.__name__
        agent_description.module = agent_class.__module__

        agent_description.description = inspect.getdoc(agent_class)

        #@note need to improve inspection of agent!
        for attr in inspect.classify_class_attrs(agent_class):
            #if attr.kind == 'method' and 'op_' in attr.name :
            if attr[1] == 'method' and 'op_' in attr[0] :

                opdesc = coi_resource_descriptions.AgentMethodInterface()
                #opdesc.name = attr.name
                opdesc.name = attr[0]
                #print 'INSEPCT',inspect.getdoc(attr.object)
                #opdesc.description = inspect.getdoc(attr.object)
                #Can't seem to get the arguments in any meaningful way...
                #opdesc.arguments = inspect.getargspec(attr.object)

                agent_description.interface.append(opdesc)
        return agent_description

    def get_agent_definition(self, agent_description_reference):
        """
        Get a agent definition
        """
        return self.base_get_resource('get_agent_definition', agent_description_reference)

    @defer.inlineCallbacks
    def register_agent_instance(self, agent, descriptor=None):
        """
        Client method to register a Agent Instance
        @param agent takes in the agent to create a class and register a new instrument
        @param descriptor The empty, partial or full storage area for additial,
            subclass-specific values.
        """
        assert ((descriptor == None) or
               (isinstance(descriptor, coi_resource_descriptions.AgentInstance)))

        if isinstance(agent, coi_resource_descriptions.AgentInstance):
            agent_resource = agent
            assert agent_resource.RegistryIdentity, 'Agent Resource must have a registry Identity'

        else:
            agent_instance = agent
            # Build a new description of this agent instance
            agent_resource = yield self.describe_instance(agent_instance, descriptor)

            found_sir = yield self.find_registered_agent_instance_from_description(agent_resource)
            if found_sir:
                assert len(found_sir) == 1
                defer.returnValue(found_sir[0])
            else:
                agent_resource.create_new_reference()
                agent_resource.set_lifecyclestate(dataobject.LCStates.developed)

        agent_resource = yield self.base_register_resource('register_agent_instance',agent_resource)
        defer.returnValue(agent_resource)

    @defer.inlineCallbacks
    def describe_instance(self, agent_instance, descriptor=None):
        """
        From an instance of an agent, generate a simple description object
        @param agent_instance should be of type ResourceAgent
        @param descriptor The instance resource description that will have
            its core values from the AgentInstance parent class filled out.
            For example, this will be an instantiated object of type
            ion.resources.ipaa_resource_descriptions.InstrumentAgentResourceInstance
        """
        assert((descriptor == None) or
               (isinstance(descriptor,
                           coi_resource_descriptions.AgentInstance)))

        # Do not make a new resource idenity - this is a generic method which
        # is also used to look for an existing description
        if (descriptor == None):
            agent_resource = coi_resource_descriptions.AgentInstance()
        else:
            agent_resource = descriptor

        agent_class = agent_instance.__class__

        sd = yield self.register_agent_definition(agent_class)
        agent_resource.description = sd.reference(head=True)

        #if agent_instance.id:
        #    agent_resource.process_id = agent_instance.id
        if agent_instance.proc_name:
            agent_resource.proc_name = agent_instance.proc_name
        if agent_instance.spawn_args:
            agent_resource.spawn_args = agent_instance.spawn_args
        agent_resource.process_state = agent_instance._get_state()

        # add a reference to the supervisor - can't base process does not have the same fields as ProcessDesc
        #if agent_resource.sup_process:
        #    print agent_instance.sup_process.__dict__
        #    sr = yield self.register_agent_instance(agent_instance.sup_process)
        #    agent_resource.sup_process = sr.reference(head=True)

        if agent_instance.name:
            agent_resource.name = agent_instance.name

        defer.returnValue(agent_resource)

    def get_agent_instance(self, agent_reference):
        """
        Get a agent instance
        """
        return self.base_get_resource('get_agent_instance',agent_reference)

    def set_agent_lcstate(self, agent_reference, lcstate):
        return self.base_set_resource_lcstate('set_agent_lcstate',agent_reference, lcstate)

    def set_agent_lcstate_new(self, agent_reference):
        return self.set_agent_lcstate(agent_reference, dataobject.LCStates.new)

    def set_agent_lcstate_active(self, agent_reference):
        return self.set_agent_lcstate(agent_reference, dataobject.LCStates.active)

    def set_agent_lcstate_inactive(self, agent_reference):
        return self.set_agent_lcstate(agent_reference, dataobject.LCStates.inactive)

    def set_agent_lcstate_decomm(self, agent_reference):
        return self.set_agent_lcstate(agent_reference, dataobject.LCStates.decomm)

    def set_agent_lcstate_retired(self, agent_reference):
        return self.set_agent_lcstate(agent_reference, dataobject.LCStates.retired)

    def set_agent_lcstate_developed(self, agent_reference):
        return self.set_agent_lcstate(agent_reference, dataobject.LCStates.developed)

    def set_agent_lcstate_commissioned(self, agent_reference):
        return self.set_agent_lcstate(agent_reference, dataobject.LCStates.commissioned)

    @defer.inlineCallbacks
    def find_registered_agent_definition_from_agent(self, agent_class):
        """
        Find the definition of a agent
        """
        agent_description = self.describe_agent(agent_class)

        alist = yield self.base_find_resource('find_registered_agent_definition_from_agent', agent_description,regex=False,ignore_defaults=True)
        # Find returns a list but only one agent should match!
        if alist:
            assert len(alist) == 1
            defer.returnValue(alist[0])
        else:
            defer.returnValue(None)

    def find_registered_agent_definition_from_description(self, agent_description,regex=True,ignore_defaults=True):
        """
        Find agent definitions which meet a description
        """
        return self.base_find_resource('find_registered_agent_definition_from_description', agent_description,regex,ignore_defaults)


    @defer.inlineCallbacks
    def find_registered_agent_instance_from_agent(self, agent_instance):
        """
        Find agent instances
        """
        agent_resource = yield self.describe_instance(agent_instance)
        alist = yield self.base_find_resource('find_registered_agent_instance_from_agent', agent_resource, regex=False,ignore_defaults=True)
        # Find returns a list but only one agent should match!
        if alist:
            assert len(alist) == 1
            defer.returnValue(alist[0])
        else:
            defer.returnValue(None)

    def find_registered_agent_instance_from_description(self, agent_instance_description,regex=True,ignore_defaults=True):
        """
        Find agent instances which meet a description
        """
        return self.base_find_resource('find_registered_agent_instance_from_description', agent_instance_description,regex,ignore_defaults)
'''