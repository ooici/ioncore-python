#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging, time
from twisted.internet import defer
from twisted.trial import unittest
from magnet.spawnable import spawn

from ion.play.hello_service import *
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class HelloTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._startContainer()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stopContainer()

    @defer.inlineCallbacks
    def test_hello(self):
       
        services = [
            {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
        ]
        
        yield self._spawnProcesses(services)
        
        sup = yield self.procRegistry.get("bootstrap")
        logging.info("Supervisor: "+repr(sup))

        hsid = yield self.procRegistry.get("hello1")
        logging.info("Hello service process 1: "+repr(hsid))

        hc = HelloServiceClient()
        yield hc.attach()
        res = yield hc.hello(hsid,"Hi there, hello1")
