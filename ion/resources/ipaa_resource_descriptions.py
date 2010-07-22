#!/usr/bin/env python
"""
@file ion/resources/ipaa_resource_descriptions.py
@author Steve Foley

Definitions for resource descriptions used within the IPAA subsystem
"""
from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, Resource, ResourceReference, InformationResource, StatefulResource
from ion.resources.coi_resource_descriptions import ResourceDescription 

class InstrumentResource(Resource):
    """
    Intended for the "Instrument Registry," some basic instrument metadata to
    hang onto for now. A few fields to start with can always be appended to
    for a more complete listing (or even subclassing as needed).
    """
    manufacturer = TypedAttribute(str)
    model = TypedAttribute(str)
    serial_num = TypedAttribute(str)
    instrument_type = TypedAttribute(str)
    fw_version= TypedAttribute(str)
    