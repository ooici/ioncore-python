#!/usr/bin/env python


"""
@file ion/services/dm/distribution/consumers/test/test_forwarding_consumer.py
@author David Stuebe
@brief test for the forwarding consumer process
"""


from twisted.trial import unittest

import logging
logging = logging.getLogger(__name__)
from ion.test.iontest import IonTestCase

from ion.services.dm.distribution.consumers import forwarding_consumer

from ion.services.dm.util import dap_tools
from twisted.internet import defer


class TestForwardingConsumer(IonTestCase):
    '''
    @Brief Test case for a consumer which provides a forwarding service only.
    This test case examines only the business logic of the ondata method which
    distributes the message to all of the keywork queues list.
    @Note this is note the most efficient way to do this but it expediante to the
    mehtod available.
    '''
    @defer.inlineCallbacks
    def setUp(self):
        
        self.dapdata = dap_tools.demo_dataset()
        
        self.dictdata = {'data':3.14159,'name':'stuff'}
        self.strdata = 'Junk in a string'
        
        self.notification='Notify me that OOI is working'
        
        self.timestamp=3.14159

        self.queues1=['abc']        
        self.queues2=['abc','def']
        
        self.fc = forwarding_consumer.ForwardingConsumer()
        yield self.fc.plc_init()
        
    def test_no_queues(self):
        self.fc.ondata(self.dictdata, self.notification, self.timestamp)
        
        self.assertEqual(self.fc.msgs_to_send,[])

    def test_dict_queues1(self):
        self.fc.ondata(self.dictdata, self.notification, self.timestamp,queues=self.queues1)
        
        self.assertEqual(len(self.fc.msgs_to_send),1)


    def test_dict_queues2(self):
        self.fc.ondata(self.dictdata, self.notification, self.timestamp,queues=self.queues2)
        
        self.assertEqual(len(self.fc.msgs_to_send),2)


    def test_dapdata(self):
        self.fc.ondata(self.dapdata, self.notification, self.timestamp,queues=self.queues1)
        self.assertEqual(len(self.fc.msgs_to_send),1)
        
    def test_strdata(self):
        self.fc.ondata(self.strdata, self.notification, self.timestamp,queues=self.queues1)
        self.assertEqual(len(self.fc.msgs_to_send),1)
        