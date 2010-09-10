#!/usr/bin/env python


"""
@file ion/services/dm/distribution/consumers/test/test_logging_consumer.py
@author David Stuebe
@brief test for the logging consumer process
"""

from twisted.trial import unittest

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from ion.test.iontest import IonTestCase

from ion.services.dm.distribution.consumers import logging_consumer

from ion.services.dm.util import dap_tools


class TestLoggingConsumer(IonTestCase):
    '''
    @Brief Test case for a simple logging consumer. This is a test for the
    business logic of the ondata method.
    @TODO How to veryify that the correct content is sent to the log?
    '''

    def setUp(self):

        self.dapdata = dap_tools.demo_dataset()

        self.dictdata1 = {'data':3.14159,'name':'stuff'}
        self.dictdata2 = {'data':3.14159,'noname':'stuff'}
        self.strdata = 'Junk in a string'

        self.notification='Notify me that OOI is working'

        self.timestamp=3.14159



    def test_dict1(self):
        lc = logging_consumer.LoggingConsumer()
        lc.ondata(self.dictdata1, self.notification, self.timestamp)

        # Is there a way to assert that the log is correct?

    def test_dict2(self):

        lc = logging_consumer.LoggingConsumer()
        lc.ondata(self.dictdata2, self.notification, self.timestamp)

    def test_dapdata(self):

        lc = logging_consumer.LoggingConsumer()
        lc.ondata(self.dapdata, self.notification, self.timestamp)

    def test_strdata(self):

        lc = logging_consumer.LoggingConsumer()
        lc.ondata(self.strdata, self.notification, self.timestamp)

    def test_strdata_empty_notification(self):

        lc = logging_consumer.LoggingConsumer()
        lc.ondata(self.strdata, '', self.timestamp)
