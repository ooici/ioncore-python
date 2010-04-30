#!/usr/bin/env python

"""
@file ion/data/test_objstore.py
@author Michael Meisinger
@brief test object store
"""

import logging
from twisted.internet import defer
from twisted.trial import unittest

from ion.data.objstore import *
from ion.test.iontest import IonTestCase



class ObjectStoreTest(unittest.TestCase):
    """Testing client classes of resource registry
    """
    
    def test_ValueObjects(self):
        vo1 = ValueObject('1')
        # Basic value objects
        print vo1
        print vo1.__dict__
        
        vo2 = ValueObject(2)
        
        vo3 = ValueObject(('a','b'))
        vo4 = ValueObject(['a','b'])
        vo5 = ValueObject({'a':'b', 'c':(1,2), 'd':{}, 'e':{'x':'y'}})
        print vo5.__dict__

        # Composite value objects with childrefs
        voc1 = ValueObject('comp1',())
        voc2 = ValueObject('comp1',(vo1.identity,))
        voc3 = ValueObject('comp1',(vo2,vo3))