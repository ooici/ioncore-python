#!/usr/bin/env python
"""
@file ion/resources/ipaa_resource_descriptions.py
@author Steve Foley

Definitions for resource descriptions used within the IPAA subsystem
"""
from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, ResourceReference, InformationResource, StatefulResource
from ion.resources.coi_resource_descriptions import ResourceDescription 

class InstrumentAgentResourceDescription(ResourceDescription):
    """
    The derived class that specifies what information the instrument agents
    are going to keep in the resource registry
    @note <class> must be a type which Python can instantiate with eval!

    att1 = TypedAttribute(<class>, default=None)
    att2 = TypedAttribute(<class>)
    """