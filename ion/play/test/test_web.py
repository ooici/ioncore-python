#!/usr/bin/env python

"""
@file ion/play/test/test_hello.py
@test ion.play.hello_service Example unit tests for sample code.
@author Michael Meisinger
"""

from twisted.internet import defer

from ion.play.web_service import WebServiceClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class HelloTest(IonTestCase):
    """
    Testing example hello service.
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
            {'name':'hello1','module':'ion.play.web_service','class':'WebService'},
        ]

        sup = yield self._spawn_processes(services)

        wc = WebServiceClient(proc=sup)
        yield wc.set_string('hello http world!')
