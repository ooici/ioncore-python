#!/usr/bin/env python

"""
@file ion/services/dm/distribution/consumers/message_count_consumer.py
@author David Stuebe
@brief Counts the number of messages received on its queue and forwards the result
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

import logging
logging = logging.getLogger(__name__)


class MessageCountConsumer(base_consumer.BaseConsumer):
    """
    Count the messages received
    """
    def ondata(self, data, notification, timestamp, queue=''):
        if not self.delivery_interval:
            raise RuntimeError('MessageCountConsumer must be called with a delivery interval')
        
    
    def onschedule(self, queue='', **kwargs):
        '''
        This method is called when it is time to actually send the results
        in this case it is not needed, but must be over-riden...
        '''
            
        # Count the messages recieved
        total = 0
        for k,v in self.receive_cnt.items():
            total += v
            
        interval = 0 
        for k,v in self.interval_cnt.items():
            interval += v
            
            
        notification = '''Message Counter has received %s messages, %s since last report''' \
                       % (total, interval)
            
        data = {'name':'MessageCountData','total':self.receive_cnt,'interval':self.interval_cnt}
            
        self.queue_result(queue,data,notification)

        

# Spawn of the process using the module name
factory = ProtocolFactory(MessageCountConsumer)
