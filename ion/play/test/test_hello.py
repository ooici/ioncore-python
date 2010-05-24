#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

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

        hc = HelloServiceClient(proc=sup)
        yield hc.hello("Hi there, hello1")
