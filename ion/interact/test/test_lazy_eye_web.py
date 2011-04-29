#!/usr/bin/env python

"""
@file ion/interact/test/test_lazy_eye_web.py
@author Paul Hubbard
@date 4/29/11
@test Test lazy eye web service. Uses simple REST URLs to start/stop/display
the service.
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from twisted.web.client import getPage

from ion.test.iontest import IonTestCase

from ion.util.procutils import asleep

from ion.util.itv_decorator import itv
from ion.core import ioninit

CONF = ioninit.config(__name__)

BASE_URL = 'http://dyn137-110-115-239.ucsd.edu:4114/'
GO_URL = BASE_URL + 'go'
STOP_URL = BASE_URL + 'stop'
DISPLAY_URL = BASE_URL + 'display'

class LEWTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @itv(CONF)
    @defer.inlineCallbacks
    def test_basic(self):
        yield getPage(GO_URL)
        yield asleep(5)
        yield getPage(STOP_URL)
        d = yield getPage(DISPLAY_URL)
        self.failIf(d is None)


        