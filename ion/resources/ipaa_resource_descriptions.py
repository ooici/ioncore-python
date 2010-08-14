#!/usr/bin/env python
"""
@file ion/resources/ipaa_resource_descriptions.py
@author Steve Foley

Definitions for resource descriptions used within the IPAA subsystem
"""
from ion.data.dataobject import TypedAttribute, ResourceReference, StatefulResource
from ion.resources.coi_resource_descriptions import ResourceDescription, AgentDescription, AgentInstance

class InstrumentAgentResourceDescription(AgentDescription):
    """
    The description of an instrument agent resource to go into the
    "Agent Registry". Should track metadata about the instrument agent
    that is running/registered.
    """

class InstrumentAgentResourceInstance(AgentInstance):
    """
    Intended for the "Agent Registry", this is the instance information for
    an instrument agent that is running. This should reflect the individual
    instrument agent information.
    """
    driver_process_id = TypedAttribute(str)
    instrument_ref = TypedAttribute(ResourceReference)

class InstrumentDriverResource(StatefulResource):
    """
    Intended for the "Agent Registry", this is the instance information for
    an instrument driver that is running. This should reflect the individual
    instrument driver information.
    """
    # The ID of the instrument the driver interacts with
    instrument_instance = TypedAttribute(str)
    description = TypedAttribute(ResourceReference)
    #owner = TypedAttribute(ResourceReference)
    spawnargs = TypedAttribute(str)
    type = TypedAttribute(str)
    process_id = TypedAttribute(str)
    subject = TypedAttribute(ResourceReference)
    data_sets = TypedAttribute(list)


class InstrumentResourceDescription(ResourceDescription):
    """
    Indended for the "Instrument Registry", this is some basic metadata common
    to a class of all instrument resources in the system.
    """
    # The type of instrument we are registering
    instrument_type = TypedAttribute(str)
