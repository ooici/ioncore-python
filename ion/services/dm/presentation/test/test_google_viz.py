#!/usr/bin/env python

"""
@file ion/services/dm/presentation/test/test_google_viz.py
@author David Stuebe
"""

from twisted.internet import defer
from twisted.internet import error

from ion.test.iontest import IonTestCase
from twisted.web import client
from twisted.trial import unittest

from ion.services.dm.presentation import google_viz_consumer

class GoogleVizWebTest(unittest.TestCase):
    """
    Testing web service, startup/shutdown hooks
    """

    @defer.inlineCallbacks
    def test_no_data_yet(self):
        #raise unittest.SkipTest('Port causes error on buildbot...') 

        gviz = google_viz_consumer.GoogleVizConsumer()
        gviz.params={}
        gviz.customize_consumer()
        
        page = yield client.getPage('http://127.0.0.1:2100/')

        self.failUnlessSubstring('No data yet...', page)
        
        yield gviz.plc_shutdown()


    @defer.inlineCallbacks
    def test_set_port(self):
        #raise unittest.SkipTest('Port causes error on buildbot...') 

        gviz = google_viz_consumer.GoogleVizConsumer()
        gviz.params={'port':8080}
        gviz.customize_consumer()
        
        page = yield client.getPage('http://127.0.0.1:8080/')

        self.failUnlessSubstring('No data yet...', page)
        
        yield gviz.plc_shutdown()

    @defer.inlineCallbacks
    def test_two_consumers(self):
        #raise unittest.SkipTest('Port causes error on buildbot...') 

        gviz1 = google_viz_consumer.GoogleVizConsumer()
        gviz1.params={'port':8080}
        gviz1.customize_consumer()
        
        page = yield client.getPage('http://127.0.0.1:8080/')
        self.failUnlessSubstring('No data yet...', page)

        gviz2 = google_viz_consumer.GoogleVizConsumer()
        gviz2.params={'port':2100}
        gviz2.customize_consumer()
        
        page = yield client.getPage('http://127.0.0.1:8080/')
        self.failUnlessSubstring('No data yet...', page)
        
        page = yield client.getPage('http://127.0.0.1:2100/')
        self.failUnlessSubstring('No data yet...', page)
        
        yield gviz1.plc_shutdown()
        yield gviz2.plc_shutdown()

    @defer.inlineCallbacks
    def test_two_consumers_bad_port(self):
        #raise unittest.SkipTest('Port causes error on buildbot...') 

        gviz1 = google_viz_consumer.GoogleVizConsumer()
        gviz1.params={'port':8080}
        gviz1.customize_consumer()
        
        page = yield client.getPage('http://127.0.0.1:8080/')
        self.failUnlessSubstring('No data yet...', page)

        gviz2 = google_viz_consumer.GoogleVizConsumer()
        gviz2.params={'port':8080}
        self.failUnlessRaises(error.CannotListenError, gviz2.customize_consumer)
        
        page = yield client.getPage('http://127.0.0.1:8080/')
        self.failUnlessSubstring('No data yet...', page)
        
        
        yield gviz1.plc_shutdown()
        yield gviz2.plc_shutdown()
        



