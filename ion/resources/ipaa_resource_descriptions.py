#!/usr/bin/env python
"""
@file ion/resources/ipaa_resource_descriptions.py
@author Steve Foley

Definitions for resource descriptions used within the IPAA subsystem
"""
from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, ResourceReference, InformationResource, StatefulResource
from ion.resources.coi_resource_descriptions import ResourceDescription 

class AgentMethodInterface(StatefulResource):
    description = TypedAttribute(str)
    arguments = TypedAttribute(str)

class AgentDescription(StatefulResource):
    """
    Resource Descriptions are stored in the resource registry.
    They describe resources, resource types and resource attributes
    """
    interface = TypedAttribute(list)
    module = TypedAttribute(str)
    version = TypedAttribute(str)
    #spawnargs = TypedAttribute(dict,{})
    description = TypedAttribute(str)
    
class AgentInstance(StatefulResource):
    """
    Resource Instances are stored in the resource registry.
    They describe instances of a resource type
    """
    description = TypedAttribute(ResourceReference)
    #owner = TypedAttribute(ResourceReference)
    spawnargs = TypedAttribute(str)
    type = TypedAttribute(str)
    process_id = TypedAttribute(str)
