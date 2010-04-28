#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging
from twisted.internet import defer
from twisted.trial import unittest

from ion.services.coi.resource_registry import *
from ion.test.iontest import IonTestCase



class ResourceRegistryClientTest(unittest.TestCase):
    """Testing client classes of resource registry
    """
    
    def test_ResourceDesc(self):
        # Instantiate without args and then set
        rd1 = ResourceDesc()
        rd1.setResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)

        # Instantiate with args
        rd2 = ResourceDesc(name='res2',res_type=ResourceTypes.RESTYPE_GENERIC)

        # Instantiate with name only
        rd3 = ResourceDesc(res_type=ResourceTypes.RESTYPE_GENERIC)
    
    def test_ResourceTypeDesc(self):
        # Instantiate without args
        rtd1 = ResourceTypeDesc()
        rtd1.setResourceTypeDesc(name='gen',res_type=ResourceTypes.RESTYPE_GENERIC)
        print "Object identity "+str(rtd1.identity)
        
        self.assertEqual(rtd1.name,'gen')
        self.assertEqual(rtd1.res_type,ResourceTypes.RESTYPE_GENERIC)

        rtd2 = ResourceTypeDesc(name='svc',res_type=ResourceTypes.RESTYPE_SERVICE)
        self.assertEqual(rtd2.name,'svc')
        self.assertEqual(rtd2.res_type,ResourceTypes.RESTYPE_SERVICE)

        rtd3 = ResourceTypeDesc(name='new',based_on=ResourceTypes.RESTYPE_GENERIC)
        self.assertEqual(rtd3.name,'new')
        self.assertEqual(rtd3.based_on,ResourceTypes.RESTYPE_GENERIC)
        self.assertEqual(rtd3.res_type,ResourceTypes.RESTYPE_UNASSIGNED)
        
class ResourceRegistryTest(IonTestCase):
    """Testing service classes of resource registry
    """

    def setUp(self):
        IonTestCase.setUp(self)

    def tearDown(self):
        IonTestCase.tearDown(self)
   
    @defer.inlineCallbacks
    def test_serviceReg(self):
        yield self._startMagnet()
        yield self._startCoreServices()
        
        rd2 = ResourceDesc(name='res2',res_type=ResourceTypes.RESTYPE_GENERIC)
        c = ResourceRegistryClient()
        rid = yield c.registerResource(rd2)
        logging.info('Resource registered with id '+str(rid))

        rd3 = yield c.getResourceDesc(rid)
        logging.info('Resource desc '+str(rd3))
        self.assertEqual(rd3.res_name,'res2')

        rd4 = yield c.getResourceDesc('NONE')
        self.assertFalse(rd4,'resource desc not None')
        
        yield self._stopMagnet()
       
        