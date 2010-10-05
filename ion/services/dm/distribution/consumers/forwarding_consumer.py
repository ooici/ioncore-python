#!/usr/bin/env python



"""
@file ion/services/dm/distribution/consumers/forwarding_consumer.py
@author David Stuebe
@brief Simple consumer to forward messages. This is not the correct way to
distribute messages between queues, but it is expediante to have such a method
for demonstration purposes. This is not currently an accessible pattern from the
pubsub subscription service - a delivery queue argument can have at most one
queue due to the way consumers are created from a work flow.
"""

from ion.services.dm.distribution import base_consumer
from ion.core.base_process import ProcessFactory


class ForwardingConsumer(base_consumer.BaseConsumer):
    """
    This is an simple consumer to forward messages to 'queues' from spawn_args,
    'delivery queues' which is passed as **kwargs to ondata
    """
    def ondata(self, data, notification, timestamp, queues=[]):
        
        if not hasattr(queues,'__iter__'):
            queues = [queues]
        
        for queue in queues:
            self.queue_result(queue,data,notification)

# Spawn of the process using the module name
factory = ProcessFactory(ForwardingConsumer)
