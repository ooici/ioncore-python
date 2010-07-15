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