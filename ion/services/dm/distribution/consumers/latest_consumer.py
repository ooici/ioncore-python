#!/usr/bin/env python

"""
@file ion/services/dm/distribution/consumers/latest_consumer.py
@author David Stuebe
@brief Simple digest consumer which forwards the latest messages. 
"""

from ion.services.dm.distribution import base_consumer
from ion.core.base_process import ProcessFactory


class LatestConsumer(base_consumer.BaseConsumer):
    """
    This is an simple consumer to forward the latest messages at most once every
    'delivery interval' to 'queues'
    from spawn_args, 'delivery queues' which is passed as **kwargs to ondata
    """
    def ondata(self, data, notification, timestamp, queues=[]):
        
        # Wipe the list of messages to send then add the latest ones
        self.msgs_to_send=[]
        
        # Queue new messages to send
        if not hasattr(queues,'__iter__'):
            queues = [queues]
        
        for queue in queues:
            self.queue_result(queue,data,notification)

    def onschedule(self, **kwargs):
        '''
        This method is called when it is time to actually send the results
        in this case it is not needed, but must be over-riden...
        '''
        pass


# Spawn of the process using the module name
factory = ProcessFactory(LatestConsumer)
