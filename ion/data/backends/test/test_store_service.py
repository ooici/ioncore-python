#!/usr/bin/env python

"""
@file ion/play/test/test_hello.py
@test ion.play.hello_service Example unit tests for sample code.
@author Michael Meisinger
"""

from twisted.internet import defer

from ion.play.hello_service import HelloServiceClient
from ion.test.iontest import IonTestCase

class HelloTest(IonTestCase):
    """
    Testing example hello service.
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
            {'name':'hello1','module':'ion.data.backends.store_service','class':'HelloService'},
        ]

        sup = yield self._spawn_processes(services)

        hc = HelloServiceClient(proc=sup)
        yield hc.hello("Hi there, hello1")
