#!/usr/bin/env python

"""
@file ion/agents/resource_agent.py
@author Stephen Pasco
@author Michael Meisinger
@brief base class for all resource agent processes
"""

from twisted.internet import defer

from ion.core.base_process import BaseProcess
from ion.core.base_process import BaseProcessClient
from ion.data.datastore.registry import LCState

class ResourceAgent(BaseProcess):
    """
    Base class for resource agent processes
    If you are going to write a new agent process, subclass this one.
    """

    def op_get(self, content, headers, msg):
        """
        """

    def op_set(self, content, headers, msg):
        """
        """

    def op_getLifecycleState(self, content, headers, msg):
        """
        """

    def op_setLifecycleState(self, content, headers, msg):
        """
        """
        self.lifecycleState = LCState('new')

    def op_execute(self, content, headers, msg):
        """
        """

    def op_getStatus(self, content, headers, msg):
        """
        """

    def op_getCapabilities(self, content, headers, msg):
        """
        """

class ResourceAgentClient(BaseProcessClient):
    """
    A parent class to handle common resource agent requests. Consider
    subclassing this one when creating a new agent client
    """
    @defer.inlineCallbacks
    def setLifecycleState(self, value):
        """
        Set the lifecycle state of the instrument agent
        @param value A ion.services.coi.resource_registry.ResourceLCState value
        """
        
        (content, headers, msg) = yield self.rpc_send('setLifecycleState',
                                                      value)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def getLifecycleState(self):
        """
        Obtain the lifecycle state of the instrument agent
        @return A ion.services.coi.resource_registry.ResourceLCState value
        """
        (content, headers, msg) = yield self.rpc_send('getLifecycleState', '')
        defer.returnValue(content)