#!/usr/bin/env python

"""
@file ion/services/dm/presentation/test/test_web_viz.py
@author David Stuebe
"""

from twisted.internet import defer
from twisted.internet import error

from ion.test.iontest import IonTestCase
from twisted.web import client
from twisted.trial import unittest

from ion.services.dm.presentation import web_viz_consumer

class WebVizTest(unittest.TestCase):
    """
    Testing web service, startup/shutdown hooks
    """

    @defer.inlineCallbacks
    def test_no_data_yet(self):
        #raise unittest.SkipTest('Port causes error on buildbot...')

        wviz = web_viz_consumer.WebVizConsumer()
        wviz.params={}
        wviz.customize_consumer()

        page = yield client.getPage('http://127.0.0.1:2100/')

        self.failUnlessSubstring('No data yet...', page)

        yield wviz.plc_shutdown()


    @defer.inlineCallbacks
    def test_set_port(self):
        #raise unittest.SkipTest('Port causes error on buildbot...')

        wviz = web_viz_consumer.WebVizConsumer()
        wviz.params={'port':8080}
        wviz.customize_consumer()

        page = yield client.getPage('http://127.0.0.1:8080/')

        self.failUnlessSubstring('No data yet...', page)

        yield wviz.plc_shutdown()

    @defer.inlineCallbacks
    def test_two_consumers(self):
        #raise unittest.SkipTest('Port causes error on buildbot...')

        wviz1 = web_viz_consumer.WebVizConsumer()
        wviz1.params={'port':8080}
        wviz1.customize_consumer()

        page = yield client.getPage('http://127.0.0.1:8080/')
        self.failUnlessSubstring('No data yet...', page)

        wviz2 = web_viz_consumer.WebVizConsumer()
        wviz2.params={'port':2100}
        wviz2.customize_consumer()

        page = yield client.getPage('http://127.0.0.1:8080/')
        self.failUnlessSubstring('No data yet...', page)

        page = yield client.getPage('http://127.0.0.1:2100/')
        self.failUnlessSubstring('No data yet...', page)

        yield wviz1.plc_shutdown()
        yield wviz2.plc_shutdown()

    @defer.inlineCallbacks
    def test_two_consumers_bad_port(self):
        #raise unittest.SkipTest('Port causes error on buildbot...')

        wviz1 = web_viz_consumer.WebVizConsumer()
        wviz1.params={'port':8080}
        wviz1.customize_consumer()

        page = yield client.getPage('http://127.0.0.1:8080/')
        self.failUnlessSubstring('No data yet...', page)

        wviz2 = web_viz_consumer.WebVizConsumer()
        wviz2.params={'port':8080}
        self.failUnlessRaises(error.CannotListenError, wviz2.customize_consumer)

        page = yield client.getPage('http://127.0.0.1:8080/')
        self.failUnlessSubstring('No data yet...', page)


        yield wviz1.plc_shutdown()
        yield wviz2.plc_shutdown()
