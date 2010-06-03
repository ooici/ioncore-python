#!/usr/bin/env python

"""
@file ion/play/rdf_store/test/test_rdf_service.py
@test ion.play.rdf_store/rdf_service 
@author David Stuebe
@Brief Test code for the Rdf Service
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
            {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
        ]

        sup = yield self._spawn_processes(services)

        hc = HelloServiceClient(proc=sup)
        yield hc.hello("Hi there, hello1")
