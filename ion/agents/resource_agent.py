#!/usr/bin/env python

"""
@file ion/agents/resource_agent.py
@author Stephen Pasco
@author Michael Meisinger
@brief base class for all resource agent processes
"""

import logging
import uuid
from twisted.internet import defer

from ion.services.coi.resource_registry import ResourceRegistryClient
from ion.resources.coi_resource_descriptions import ResourceDescription
from ion.core.base_process import BaseProcess
from ion.core.base_process import BaseProcessClient
from ion.data.dataobject import LCState, LCStateNames

class ResourceAgent(BaseProcess):
    """
    Base class for resource agent processes
    If you are going to write a new agent process, subclass this one and
    setup a ResourceRegistryClient
    """
    res_reg_client = None
    resource_id = None
    
    @defer.inlineCallbacks
    def op_set_resource_registry_client(self, content, headers, msg):
        """
        Adds a resource registry client for the resource agent to use. This
        should be done before the resource attempts to interact with the
        resource registry.
        @param content Should contain a string with the process id for the
            resource registry that will be used
        """
        assert(isinstance(content, basestring))
        destination = content
        #logging.info("Setting resource registry client target in Resource Agent to %s, %s",
        #             destination, self.get_scoped_name('global', destination))
        logging.info("Setting resource registry client target in Resource Agent to %s",
                     destination)
        self.res_reg_client \
            = ResourceRegistryClient(target=self.get_scoped_name('global',
                                                                 destination))
        if (self.res_reg_client == False):
            yield self.reply_err(msg, "Resource registry client not set!")
        else:
            yield self.reply_ok(msg, True)

    @defer.inlineCallbacks
    def op_get_lifecycle_state(self, content, headers, msg):
        """
        Get the lifecycle state for the resource
        @retval LCState object
        @todo handle errors better
        """
        if (self.res_reg_client == None):
            yield self.reply_err(msg,
                                 "No resource registry client has been set!")
        if (self.resource_id != None):
            result = yield self.res_reg_client.get_resource(self.resource_id)
            assert(isinstance(result, ResourceDescription))
            yield self.reply_ok(msg, result.get_lifecyclestate())
        else:
            yield self.reply_err(msg, "Resource not registered!")

    @defer.inlineCallbacks
    def op_set_lifecycle_state(self, content, headers, msg):
        """
        Set the lifecycle state for the resource
        @param content Should be a list with a resource id and a string that
            can be turned into an LCState object
        """
        if (self.res_reg_client == None):
            yield self.reply_err(msg,
                                 "No resource registry client has been set!")
        assert(isinstance(content, basestring))
        state = str(content)
        assert(state in LCStateNames)
        assert(isinstance(self.res_reg_client, ResourceDescription))
        state = LCState(state)
        if (self.resource_id != None):
            result = yield self.res_reg_client.set_lcstate(self.resource_id,
                                                           state)
            if (result):
                yield self.reply_ok(msg, self.resource_id)
            else:
                yield self.reply_err(msg, \
                    "Could not set lifecycle state for %s" % self.resource_id) 
        else:
            yield self.reply_err(msg, \
              "Could not set lifecycle state. Resource %s does not exist." \
              % self.resource_id)
    
    @defer.inlineCallbacks
    def op_register_resource(self, content, headers, msg):
        """
        Registers or re-registers self in the resource registry.
        @param content Must include an encoded ResourceDescription class to
            register.
        @todo Turn initial parameter asserts into a decode check
        """
        assert(isinstance(content, list))
        assert("lifecycle" == content[0][0])
        assert("name" == content[1][0])
        if (self.res_reg_client == None):
            yield self.reply_err(msg,
                                 "No resource registry client has been set!")
        res_desc = ResourceDescription.decode(content)()
        assert(isinstance(res_desc, ResourceDescription))
        if (self.resource_id == None):
            self.resource_id = \
                yield self.res_reg_client.register_resource(str(uuid.uuid4()),
                                                            res_desc)
        else:
            result = yield self.res_reg_client.register_resource(id, res_desc)
        if (result == None):
            yield self.reply_err(msg, "Could not re-register object id %s" %id)
        else:
            yield self.reply_ok(msg, {'res_id': self.resource_id})
            
    @defer.inlineCallbacks
    def op_get_resource_description(self, content, headers, msg):
        """
        Get the resource description for this resource from the resource
        registry
        @retval The ResourceDescription object for this resource, as registered
            in the Resource Registry
        """
        if (self.resource_id != None):
            result = yield self.res_reg_client.get_resource(self.resource_id)
            yield self.reply_ok(msg, {'res_descr':result})
        else:
            yield self.reply_err(msg, {'res_descr':None})
        
    @defer.inlineCallbacks
    def op_get_resource_id(self, content, headers, msg):
        """
        Returns the resource id for the resource agent
        @todo handle case where it isnt registered yet
        @retval Resource ID if it has been registered, None if not registered
        """
        if (self.resource_id != None):
            yield self.reply_ok(msg, {'res_id': self.resource_id})
        else:
            yield self.reply_err(msg, {'res_id': None})

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
    def set_resource_registry_client(self, reg_id):
        """
        Set the resource registry client for this resource agent. This should
        be created to point to the correct process handle so that the resource
        agent can look into the registry properly.
        @param reg_id A globally scoped string with the process id for the
            resource registry
        @todo handle errors better
        """
        (content, headers, msg) = \
            yield self.rpc_send('set_resource_registry_client', reg_id)
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
            defer.returnValue(content['result'])
        else:
            defer.returnValue(False)        
    
    @defer.inlineCallbacks
    def get_resource_id(self):
        """
        Obtain the resource ID that the resource is registered with.
        """
        (content, headers, msg) = yield self.rpc_send('get_resource_id', '')
        if content['status'] == 'OK':
            defer.returnValue(True)
        else:
            defer.returnValue(False)        
    
    @defer.inlineCallbacks       
    def get_resource_description(self):
        """
        Obtain the resource description for the existing resource.
        """
        (content, headers, msg) = \
            yield self.rpc_send('get_resource_description', '')
        if content['status'] == 'OK':
            defer.returnValue(True)
        else:
            defer.returnValue(False)        
        
    @defer.inlineCallbacks
    def register_resource(self, resource_desc):
        """
        Have the resource register itself with the resource registry via
        the client that has been set via set_resource_registry_client()
        @see set_resource_registry_client()
        """
        (content, headers, msg) = \
            yield self.rpc_send('register_resource',
                                 resource_desc.encode())
        if content['status'] == 'OK':
            defer.returnValue(True)
        else:
            defer.returnValue(False)        
