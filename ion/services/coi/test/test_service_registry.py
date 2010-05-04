#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging
from twisted.internet import defer
from twisted.trial import unittest

from ion.services.coi.service_registry import *
from ion.test.iontest import IonTestCase



class ServiceRegistryClientTest(IonTestCase):
    """Testing client classes of service registry
    """
    
    @defer.inlineCallbacks
    def setUp(self):
        IonTestCase.setUp(self)
        yield self._startContainer()
        yield self._startCoreServices()

    @defer.inlineCallbacks
    def tearDown(self):
        IonTestCase.tearDown(self)
        yield self._stopContainer()
    
    def test_serviceReg(self):
        src = None