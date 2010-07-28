#!/usr/bin/env python

from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, ResourceReference, InformationResource, StatefulResource
from twisted.trial import unittest

"""
class EXAMPLE_RESOURCE(ResourceDescription):
    '''
    @Note <class> must be a type which python can instantiate with eval!
    '''
    att1 = TypedAttribute(<class>, default=None)
    att2 = TypedAttribute(<class>)
"""

class ExampleResource(StatefulResource):
    '''
    @Note <class> must be a type which python can instantiate with eval!
    '''
    att1 = TypedAttribute(int, default=None)
    att2 = TypedAttribute(str)

class TestResource(unittest.TestCase):
    def test_print(self):
        res = ExampleResource()
        print res
        
class InstrumentResource(StatefulResource):
    '''
    @Note <class> must be a type which python can instantiate with eval!
    '''
    baudrate = TypedAttribute(int, default=9600)
    outputformat =TypedAttribute(int, default=0)
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
            
          