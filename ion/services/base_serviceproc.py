#!/usr/bin/env python

"""
@file ion/agents/resource_agent.py
@author Michael Meisinger
@package ion.services abstract superclass for all service processes
"""

from ion.core.base_process import BaseProcess

class BaseServiceProcess(BaseProcess):
    """
    This is the abstract superclass for all service processes.
    
    A service process is a Capability Container process that can be spawned
    anywhere in the network and that provides a service.
    """
