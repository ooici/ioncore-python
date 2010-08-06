#!/usr/bin/env python



"""
@file ion/services/dm/distribution/consumers/forwarding_consumer.py
@author David Stuebe
@brief Simple consumer to forward messages. This is not the correct way to
distribute messages between queues, but it is expediante to have such a method
for demonstration purposes.
"""

from ion.services.dm.distribution import base_consumer
from ion.core.base_process import ProtocolFactory


class ForwardingConsumer(base_consumer.BaseConsumer):
    """
    This is an simple consumer to forward messages to 'queues' from spawn_args,
    'process parameters' which is passed as **kwargs to ondata
    """
    def ondata(self, data, notification, timestamp, queues=[]):
        
        for queue in queues:
            self.queue_result(queue,data,notification)

# Spawn of the process using the module name
factory = ProtocolFactory(ForwardingConsumer)
