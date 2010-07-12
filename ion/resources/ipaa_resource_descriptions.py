#!/usr/bin/env python
"""
@file ion/resources/ipaa_resource_descriptions.py
@author Steve Foley

Definitions for resource descriptions used within the IPAA subsystem
"""

from ion.data.dataobject import ResourceDescription

class InstrumentAgentResourceDescription(ResourceDescription):
    """
    The derived class that specifies what information the instrument agents
    are going to keep in the resource registry
    """