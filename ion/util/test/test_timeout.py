#!/usr/bin/env python

"""
@file ion/util/test/test_timeout.py
@author David Stuebe
"""

from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python import failure

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet.task import deferLater
from ion.util import timeout


class TimeoutTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_timeout(self):

        @timeout.timeout(1.0)
        def my_sleep(secs):
            def f():
                pass
            d = deferLater(reactor, secs, f)
            print d.callback
            return d

        yield my_sleep(0.5)


        yield self.failUnlessFailure(my_sleep(2.0),timeout.TimeoutError)


    @defer.inlineCallbacks
    def test_timeout_excpetion(self):

        @timeout.timeout(1.0)
        def my_sleep(secs):
            def f():
                raise RuntimeError('Shit happens!')
            d = deferLater(reactor, secs, f)
            return d

        yield self.failUnlessFailure(my_sleep(0.5),RuntimeError)

        yield self.failUnlessFailure(my_sleep(2.0),timeout.TimeoutError)

