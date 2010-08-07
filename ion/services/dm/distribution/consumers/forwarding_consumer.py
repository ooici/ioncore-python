#!/usr/bin/env python



"""
@file ion/services/dm/distribution/consumers/forwarding_consumer.py
@author David Stuebe
@brief simple consumer to forward messages
"""

from ion.services.dm.distribution import base_consumer
from ion.core.base_process import ProtocolFactory


class ForwardingConsumer(base_consumer.BaseConsumer):
    
    def ondata(self, data, notification, timestamp, queues=[]):
        """
        This is an simple consumer to forward messages to 'queues' from spawn_args,
        'process parameters' which is passed as **kwargs to ondata
        """
        for queue in queues:
            self.queue_result(queue,data,notification)

# Spawn of the process using the module name
factory = ProtocolFactory(ForwardingConsumer)
