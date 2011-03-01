#!/usr/bin/env python

"""
@file ion/play/test/test_hello.py
@test ion.play.hello_service Example unit tests for sample code.
@author Michael Meisinger
"""
import time


from twisted.internet import defer

from ion.play.hello_service import HelloServiceClient
from ion.test.iontest import IonTestCase
import ion.util.ionlog
from ion.core import ioninit

from ion.util.itv_decorator import itv

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

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

    @itv(CONF)
    @defer.inlineCallbacks
    def test_hello_performance(self):
        self.timeout = 60

        services = [
            {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
        ]

        sup = yield self._spawn_processes(services)

        hc = HelloServiceClient(proc=sup)

        count = 500
        tzero = time.time()

        d = []
        for x in xrange(count):
            d.append(hc.hello("Hi there, hello1"))

        yield defer.DeferredList(d)

        delta_t = (time.time() - tzero)
        print('%f elapsed, %f per second' % (delta_t, float(count) / delta_t) )

    @defer.inlineCallbacks
    def test_hello(self):
        services = [
            {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
        ]

        sup = yield self._spawn_processes(services)

        hc = HelloServiceClient(proc=sup)

        yield hc.hello("Hi there, hello1")

                

