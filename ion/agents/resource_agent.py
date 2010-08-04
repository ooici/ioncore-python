#!/usr/bin/env python

"""
@file ion/agents/resource_agent.py
@author Stephen Pasco
@author Michael Meisinger
@brief base class for all resource agent processes
"""

import logging
from twisted.internet import defer

from ion.services.coi.agent_registry import AgentRegistryClient
#from ion.resources.ipaa_resource_descriptions import InstrumentAgentResourceInstance, AgentInstance
from ion.resources.coi_resource_descriptions import AgentDescription
from ion.resources.coi_resource_descriptions import AgentInstance
from ion.core.base_process import BaseProcess
from ion.core.base_process import BaseProcessClient
from ion.data.dataobject import LCState, LCStateNames

class ResourceAgent(BaseProcess):
    """
    Base class for resource agent processes
    If you are going to write a new agent process, subclass this one and
    setup a ResourceRegistryClient
    """
    """
    The Agent Registry client class to hang onto for all registry manipulations
    """
    reg_client = None
    
    """
    Our reference object in the Agent Registry
    """
    resource_ref = None
    
    """
    This is what makes us unique for now. When we register, this is our
    handle
    """
    name = None
    
    @defer.inlineCallbacks
    def op_set_registry_client(self, content, headers, msg):
        """
        Adds a agent registry client for the resource agent to use. This
        should be done before the resource attempts to interact with the
        agent registry.
        @param content Should contain a string with the process id for the
            agent registry that will be used
        """
        assert(isinstance(content, basestring))
        destination = content
        #logging.info("Setting agent registry client target in Resource Agent to %s, %s",
        #             destination, self.get_scoped_name('global', destination))
        logging.info("Setting agent registry client target in Resource Agent to %s",
                     destination)
        self.reg_client \
            = AgentRegistryClient(target=self.get_scoped_name('global',
                                                              destination))
        if (self.reg_client == False):
            yield self.reply_err(msg, "agent registry client not set!")
        else:
            yield self.reply_ok(msg, True)

    @defer.inlineCallbacks
    def op_get_lifecycle_state(self, content, headers, msg):
        """
        Get the lifecycle state for the resource
        @retval LCState string
        @todo handle errors better
        """
        if (self.reg_client == None):
            yield self.reply_err(msg,
                                 "No agent registry client has been set!")
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
        if (self.reg_client == None):
            yield self.reply_err(msg,
                                 "No agent registry client has been set!")
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
            else:
                yield self.reply_err(msg, \
                    "Could not set lifecycle state for %s" \
                        % self.resource_ref.name) 
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
        """
        if (self.reg_client == None):
            yield self.reply_err(msg,
                                 "No agent registry client has been set!")
        logging.debug("*** content: %s", content)
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
        
class ResourceAgentClient(BaseProcessClient):
    """
    A parent class to handle common resource agent requests. Consider
    subclassing this one when creating a new agent client
    """
    
    @defer.inlineCallbacks
    def set_registry_client(self, reg_id):
        """
        Set the agent registry client for this resource agent. This should
        be created to point to the correct process handle so that the resource
        agent can look into the registry properly.
        @param reg_id A globally scoped string with the process id for the
            agent registry
        @todo handle errors better
        """
        (content, headers, msg) = \
            yield self.rpc_send('set_registry_client', reg_id)
        if content['status'] == 'OK':
            defer.returnValue(True)
        else:
            defer.returnValue(False)        

        
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
        if content['status'] == 'OK':
            defer.returnValue(value)
        else:
            defer.returnValue(False)        
   
    @defer.inlineCallbacks
    def get_lifecycle_state(self):
        """
        Obtain the lifecycle state of the resource agent
        @return A ion.data.datastore.registry.LCState value
        """
        (content, headers, msg) = yield self.rpc_send('get_lifecycle_state',
                                                      '')
        if content['status'] == 'OK':
            defer.returnValue(LCState(content['value']))
        else:
            defer.returnValue(False)        
    
    @defer.inlineCallbacks
    def get_resource_ref(self):
        """
        Obtain the resource ID that the resource is registered with.
        """
        (content, headers, msg) = yield self.rpc_send('get_resource_ref', '')
        if content['status'] == 'OK':
            defer.returnValue(AgentInstance.decode(content['value']))
        else:
            defer.returnValue(None)        
    
    @defer.inlineCallbacks
    def get_resource_instance(self):
        """
        Obtain the resource instance object from the existing registered
        resource.
        """
        (content, headers, msg) = \
            yield self.rpc_send('get_resource_instance', '')
        if content['status'] == 'OK':
            content_decode = AgentInstance.decode(content['value'])
            assert(isinstance(content_decode, AgentInstance))
            defer.returnValue(content_decode)
        else:
            defer.returnValue(None)  
          
    @defer.inlineCallbacks
    def register_resource(self, agent_instance=None, descriptor=None):
        """
        Have the resource register itself with the agent registry via
        the client that has been set via set__client()
        @param resource_desc The ResourceDescription object to register
        @param resource_inst The instance object to register
        """
        if (agent_instance == None):
            (content, headers, msg) = \
                yield self.rpc_send('register_resource', '')
        else:
            assert(isinstance(agent_instance,
                              (AgentInstance, AgentDescription)))
            (content, headers, msg) = \
              yield self.rpc_send('register_resource', agent_instance.encode())
        
        if (content['status'] == 'OK'):
            defer.returnValue(AgentInstance.decode(content['value']))
        else:
            defer.returnValue(None)        
