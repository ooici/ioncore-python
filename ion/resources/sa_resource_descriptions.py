#!/usr/bin/env python
"""
@file ion/resources/sa_resource_descriptions.py
"""
from twisted.trial import unittest

from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, ResourceReference, InformationResource, StatefulResource
from ion.resources.coi_resource_descriptions import ResourceDescription, ResourceInstance
    
class InstrumentResource(StatefulResource):
    '''
    Intended for the "Instrument Registry," some basic instrument metadata to
    hang onto for now. A few fields to start with can always be appended to
    for a more complete listing (or even subclassing as needed).
    @todo Flesh this out with much much much more comprehensive metadata
    '''
    manufacturer = TypedAttribute(str)
    model = TypedAttribute(str)
    serial_num = TypedAttribute(str)
    fw_version= TypedAttribute(str)

    '''
    These are things that can be used to talk to an instrument in the CI
    '''
    agent_message_address = TypedAttribute(str)
    agent_event_address = TypedAttribute(str)
    

class SBE49InstrumentDescription(ResourceDescription):
    """
    This has the metadata common to SBE49 instruments
    """
    manufacturer = TypedAttribute(str)
    model = TypedAttribute(str)
    """
    And eventually add things that characterize the class of instrument such as
    power_draw, precision, accuracy, etc.
    """
    
class SBE49InstrumentResource(InstrumentResource):
    """
    The stuff specific to SBE49s so that you can actually talk to it enough
    to get more information for now.
    """
    baudrate = TypedAttribute(int, default=9600)
    outputformat =TypedAttribute(int, default=0)
    description = TypedAttribute(ResourceReference) #reference the description
    
    """
    This stuff is already in the instrument, so to keep it here opens the
    gotta-keep-it-in-sync can of worms that we will eventually get to.
    
    outputsal = TypedAttribute(bool, default=True)
    outputsv = TypedAttribute(bool, default=True)
    navg = TypedAttribute(int, default=0)
    mincondfreq = TypedAttribute(int, default=0)
    pumpdelay = TypedAttribute(int, default=0)
    tadvance = TypedAttribute(float, default=0.0625)
    alpha = TypedAttribute(float, default=0.03)
    tau = TypedAttribute(float, default=7.0)
    autorun = TypedAttribute(bool, default=True)
    tcaldate = TypedAttribute(str, default="1/1/01")
    ta0 = TypedAttribute(float, default=0.0)
    ta1 = TypedAttribute(float, default=0.0)
    ta2 = TypedAttribute(float, default=0.0)
    ta3 = TypedAttribute(float, default=0.0)
    toffset = TypedAttribute(float, default=0.0)
    ccaldate = TypedAttribute(str, default="1/1/01")
    cg = TypedAttribute(float, default=0.0)
    ch = TypedAttribute(float, default=0.0)
    ci = TypedAttribute(float, default=0.0)      
    cj = TypedAttribute(float, default=0.0)
    cpcor = TypedAttribute(float, default=0.0)
    ctcor = TypedAttribute(float, default=0.0)
    cslope = TypedAttribute(float, default=0.0)
    pcaldate = TypedAttribute(str, default="1/1/01")
    prange = TypedAttribute(float, default=100.0)
    poffset = TypedAttribute(float, default=0.0)
    pa0 = TypedAttribute(float, default=0.0)
    pa1 = TypedAttribute(float, default=0.0)
    pa2 = TypedAttribute(float, default=0.0)
    ptempa0 = TypedAttribute(float, default=0.0)
    ptempa1 = TypedAttribute(float, default=0.0)
    ptempa2 = TypedAttribute(float, default=0.0)
    ptca0 = TypedAttribute(float, default=0.0)
    ptca1 = TypedAttribute(float, default=0.0)
    ptca2 = TypedAttribute(float, default=0.0)
    ptcb0 = TypedAttribute(float, default=0.0)
    ptcb1 = TypedAttribute(float, default=0.0)
    ptcb2 = TypedAttribute(float, default=0.0)
    """
    