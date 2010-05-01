#!/usr/bin/env python

"""
@file ion/agents/resource_agent.py
@author Stephen Pasco
@author Michael Meisinger
@brief base class for all resource agent processes
"""

from ion.core.base_process import BaseProcess
from ion.services.coi.resource_registry import ResourceLCState
   
class ResourceAgent(BaseProcess):
    """Base class for agent processes
    If you are going to write a new agent process, start here.
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
        self.lifecycleState = ResourceLCState.RESLCS_NEW
    
    def op_execute(self, content, headers, msg):
        """
        """
    
    def op_getStatus(self, content, headers, msg):
        """
        """
    
    def op_getCapabilities(self, content, headers, msg):
        """
        """
