#!/usr/bin/env python

"""
@file ion/agents/resource_agent.py
@author Stephen Pasco
@package ion.agents abstract superclass for all agents
"""

from ion.core.base_process import BaseProcess
   
class ResourceAgent(BaseProcess):

    lifecycleState = "undeveloped"

    """
    This is the abstract superclass for all agents.
    
    If you are going to write a new agent, start here.
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
    
    def op_execute(self, content, headers, msg):
        """
        """
    
    def op_getStatus(self, content, headers, msg):
        """
        """
    
    def op_getCapabilities(self, content, headers, msg):
        """
        """

"""
Do these need to be a class or a type or enumerated something?
 lifecycleStates = ("undeveloped", "developed", "commissioned",
                            "active", "inactive", "not_acquired", "acquired")
"""
