#!/usr/bin/env python

"""
@file ion/agents/resource_agent.py
@author Stephen Pasco
@author Michael Meisinger
@author David Stuebe
@brief base class for all resource agent processes
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.services.coi.agent_registry import AgentRegistryClient
#from ion.resources.ipaa_resource_descriptions import InstrumentAgentResourceInstance, AgentInstance
from ion.resources.coi_resource_descriptions import AgentDescription
from ion.resources.coi_resource_descriptions import AgentInstance
from ion.core.process.process import Process
from ion.core.process.process import ProcessClient
from ion.data.dataobject import LCState, LCStateNames

class ResourceAgent(Process):
    """
    Base class for resource agent processes
    If you are going to write a new agent process, subclass this one and
    setup a ResourceRegistryClient
    """

    def plc_init(self):
        """
        The Agent Registry client class to hang onto for all registry manipulations
        """
        self.reg_client = AgentRegistryClient(proc=self)

        """
        Our reference object in the Agent Registry
        """
        self.resource_ref = None

        """
        This is what makes us unique for now. When we register, this is our
        handle
        """
        self.name = None


    @defer.inlineCallbacks
    def op_get_lifecycle_state(self, content, headers, msg):
        """
        Get the lifecycle state for the resource
        @retval LCState string
        @todo handle errors better
        """
        if (self.resource_ref != None):
            result = \
                yield self.reg_client.get_agent_instance(self.resource_ref)
            assert(isinstance(result, AgentInstance))
            state = result.get_lifecyclestate()
            yield self.reply_ok(msg, str(state))
        else:
            yield self.reply_err(msg, "Resource not registered!")

    @defer.inlineCallbacks
    def op_set_lifecycle_state(self, content, headers, msg):
        """
        Set the lifecycle state for the resource
        @param content Should be a list with a resource id and a string that
            can be turned into an LCState object
        """
        assert(isinstance(content, basestring))
        state = str(content)
        assert(state in LCStateNames)
        state = LCState(state)
        if (self.resource_ref != None):
            result = yield self.reg_client.set_agent_lcstate(self.resource_ref,
                                                             state)
            self.resource_ref = result.reference(head=True)
            if (result):
                yield self.reply_ok(msg, str(state))
                return
            else:
                yield self.reply_err(msg, \
                    "Could not set lifecycle state for %s" \
                        % self.resource_ref.name)
                return
        else:
            yield self.reply_err(msg, \
              "Could not set lifecycle state. Resource %s does not exist." \
              % self.resource_ref.name)

    @defer.inlineCallbacks
    def op_register_resource(self, content, headers, msg):
        """
        Registers or re-registers self in the agent registry.
        @param content Must include an encoded AgentInstance subclass that may
            or may not have been previously filled out. The instance class
            should be appropriate to the type of resource being registered.
            Perhaps the client is checking the type?
        @todo Turn initial parameter asserts into a decode check
        @note Registering an existing InstrumentAgent with an existing registry
        @msc
       	hscale = "2";
	 User, InstrumentAgentClient, InstrumentAgentResourceInstance, InstrumentAgent, ResourceAgent, ResourceAgentClient, AgentRegistryClient, AgentRegistry;
         User -> InstrumentAgent [label="instantiate and spawn IA subclass"];
         User -> InstrumentAgentClient[label="instantiate IAClient subclass"];
         AgentRegistryClient -> InstrumentAgent [label="reference stored in IA"];
         --- [label="All setup now, registry must already have a client on hand"];
         User => InstrumentAgentClient [label="register_resource()"];
         InstrumentAgentClient -> InstrumentAgentResourceInstance [label="instantiate"];
         InstrumentAgentClient -> InstrumentAgentResourceInstance [label="get driver address"];
         InstrumentAgentClient <- InstrumentAgentResourceInstance [label="driver address"];
         InstrumentAgentClient => InstrumentAgentClient [label="ResourceAgentClient.register_resource()"];
         InstrumentAgentClient =>> InstrumentAgent [label="op_register_resource() via AMQP"];
         InstrumentAgent => AgentRegistryClient [label="register_agent_instance(self, custom_descriptor)"];
         AgentRegistryClient => AgentRegistryClient [label="describe_instance(agent_instance, custom_descriptor)"];
         AgentRegistryClient << AgentRegistryClient [label="return AgentDescription"];
         AgentRegistryClient => AgentRegistryClient [label="find_registered_agent_instance_from_description(above)"];
         AgentRegistryClient =>> AgentRegistry [label="register_resource() via base class AMQP"];
         AgentRegistryClient <<= AgentRegistry [label="return via AMQP"];
         InstrumentAgent << AgentRegistryClient [label="Success/failure"];
         InstrumentAgentClient <<= ResourceAgent [label="Success/failure via InstrumentAgent via AMQP"];
         User << InstrumentAgentClient [label="return"];
        @endmsc
        """
        if (content == ""):
            descriptor = None
        elif (content != None):
            descriptor = AgentInstance.decode(content)
        assert((descriptor == None) or (isinstance(descriptor, AgentInstance)))
        assert(descriptor != "")
        # Register the instance/description
        returned_instance = \
            yield self.reg_client.register_agent_instance(self, descriptor)
        self.resource_ref = returned_instance.reference(head=True)
        if (self.resource_ref == None) or (self.resource_ref == False):
            yield self.reply_err(msg, "Could not register instance!")
        else:
            yield self.reply_ok(msg, self.resource_ref.encode())

    @defer.inlineCallbacks
    def op_get_resource_instance(self, content, headers, msg):
        """
        Get the resource instance for this resource from the agent registry
        @retval Via messageg, send the resource instance object for this
            resource, as registered in the agent registry
        """
        if (self.resource_ref != None):
            result = \
                yield self.reg_client.get_agent_instance(self.resource_ref)
            assert(isinstance(result, AgentInstance))
            yield self.reply_ok(msg, result.encode())
        else:
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks
    def op_get_resource_ref(self, content, headers, msg):
        """
        Returns the resource id for the resource agent
        @todo handle case where it isnt registered yet
        @retval An encoded resource reference if the resource has been
            registered, None if not registered
        """
        if (self.resource_ref != None):
            yield self.reply_ok(msg, self.resource_ref.encode())
        else:
            yield self.reply_err(msg, None)

    def op_get(self, content, headers, msg):
        """
        Abstract method for the Resource Agent interface
        """

    def op_set(self, content, headers, msg):
        """
        Abstract method for the Resource Agent interface
        """

    def op_execute(self, content, headers, msg):
        """
        Abstract method for the Resource Agent interface
        """

    def op_get_status(self, content, headers, msg):
        """
        Abstract method for the Resource Agent interface
        """

    def op_get_capabilities(self, content, headers, msg):
        """
        Abstract method for the Resource Agent interface
        """

class ResourceAgentClient(ProcessClient):
    """
    A parent class to handle common resource agent requests. Consider
    subclassing this one when creating a new agent client
    """

    @defer.inlineCallbacks
    def set_lifecycle_state(self, value):
        """
        Set the lifecycle state of the resource agent
        @param value A ion.data.datastore.registry.LCState value
        @retval resource ID that was assigned to the resource in the registry
        @todo Push LCState object, not just the string some day?
        """
        assert(isinstance(value, LCState))
        (content, headers, msg) = yield self.rpc_send('set_lifecycle_state',
                                                      str(value))
        defer.returnValue(value)

    @defer.inlineCallbacks
    def get_lifecycle_state(self):
        """
        Obtain the lifecycle state of the resource agent
        @return A ion.data.datastore.registry.LCState value
        """
        (content, headers, msg) = yield self.rpc_send('get_lifecycle_state',
                                                      '')
        defer.returnValue(LCState(content['value']))

    @defer.inlineCallbacks
    def get_resource_ref(self):
        """
        Obtain the resource ID that the resource is registered with.
        """
        (content, headers, msg) = yield self.rpc_send('get_resource_ref', '')
        defer.returnValue(AgentInstance.decode(content['value']))

    @defer.inlineCallbacks
    def get_resource_instance(self):
        """
        Obtain the resource instance object from the existing registered
        resource.
        """
        (content, headers, msg) = \
            yield self.rpc_send('get_resource_instance', '')
        content_decode = AgentInstance.decode(content['value'])
        assert(isinstance(content_decode, AgentInstance))
        defer.returnValue(content_decode)

    @defer.inlineCallbacks
    def register_resource(self, agent_instance=None, descriptor=None):
        """
        Have the resource register itself with the agent registry via
        the client that has been set via set__client()
        @param resource_desc The ResourceDescription object to register
        @param resource_inst The instance object to register
        """
        if (agent_instance == None):
            (content, headers, msg) = yield self.rpc_send('register_resource', '')
        else:
            assert(isinstance(agent_instance, (AgentInstance, AgentDescription)))
            # Add resource agent part of the information
            agent_instance.proc_id = str(self.target)
            (content, headers, msg) = \
              yield self.rpc_send('register_resource', agent_instance.encode())

        defer.returnValue(AgentInstance.decode(content['value']))
