#!/usr/bin/env python

"""
@file ion/play/test/test_web.py
@test ion.play.web_service Startup and test web server.
@author Paul Hubbard
"""

from twisted.internet import defer

from ion.play.web_service import WebServiceClient
from ion.test.iontest import IonTestCase

class HelloTest(IonTestCase):
    """
    Testing web service, startup/shutdown hooks
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        # @note Required to trigger the slc_shutdown hook
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello(self):

        services = [
            {'name':'webs','module':'ion.play.web_service','class':'WebService'},
        ]

        sup = yield self._spawn_processes(services)

        wc = WebServiceClient(proc=sup)
        yield wc.set_string('hello http world!')

        # @todo http client to pull same...
        
