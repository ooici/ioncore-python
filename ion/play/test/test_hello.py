#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging

from twisted.internet import defer

from ion.play.hello_service import HelloServiceClient
from ion.test.iontest import IonTestCase

class HelloTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello(self):

        services = [
            {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
        ]

        sup = yield self._spawn_processes(services)
        logging.info("Supervisor: "+repr(sup))

        hsid = yield self.procRegistry.get("hello1")
        logging.info("Hello service process 1: "+repr(hsid))

        hc = HelloServiceClient()
        yield hc.attach()
        res = yield hc.hello(hsid,"Hi there, hello1")
