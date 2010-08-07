#!/usr/bin/env python


"""
@file ion/services/dm/distribution/consumers/test/test_example_consumer.py
@author David Stuebe
@brief test for the example consumer process
"""


from twisted.trial import unittest

import logging
logging = logging.getLogger(__name__)
from ion.test.iontest import IonTestCase

from ion.services.dm.distribution.consumers import example_consumer

from ion.services.dm.util import dap_tools
from twisted.internet import defer


class TestLoggingConsumer(IonTestCase):
    
    @defer.inlineCallbacks
    def setUp(self):
        
        self.data1 = dap_tools.simple_dataset(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,6,4,-2,-1,5,3,1,4,5)})
        
        self.data2 = dap_tools.simple_dataset(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(101,102,103,104,105,106,107,108,109,110), \
            'height':(5,2,9001,5,-1,9,3,888,3,4)})
        
        
        
        self.notification='Notify me that OOI is working'
        
        self.timestamp=3.14159
        
        self.ec = example_consumer.ExampleConsumer()
        yield self.ec.plc_init()
        
    def test_data1(self):
        self.ec.ondata(self.data1, 'note', 1.0, event_queue='abc',processed_queue='def')
        
        self.assertEqual(len(self.ec.msgs_to_send),3)


    def test_data2(self):
        self.ec.ondata(self.data2, 'note', 1.0, event_queue='abc',processed_queue='def')
        
        self.assertEqual(len(self.ec.msgs_to_send),4)
        