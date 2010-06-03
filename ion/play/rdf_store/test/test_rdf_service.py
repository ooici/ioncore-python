#!/usr/bin/env python

"""
@file ion/play/rdf_store/test/test_rdf_service.py
@test ion.play.rdf_store/rdf_service 
@author David Stuebe
@Brief Test code for the Rdf Service
"""

from twisted.internet import defer

from ion.play.rdf_store.rdf_service import RdfServiceClient
from ion.test.iontest import IonTestCase

class RdfServiceTest(IonTestCase):
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
            {'name':'RdfService1','module':'ion.play.rdf_store.rdf_service','class':'RdfService'},
        ]

        sup = yield self._spawn_processes(services)

        rsc = RdfServiceClient(proc=sup)
        yield rsc.push("Hi there, PushMe")
        
        yield rsc.pull("Hi there, PullMe")
