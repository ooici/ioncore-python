#!/usr/bin/env python

"""
@file ion/services/dm/datapubsub.py
@author Michael Meisinger
@brief service for publishing on data streams, and for subscribing to streams
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class DataPubsubService(BaseService):
    """Data publish/subscribe service interface
    """

    def op_define_topic(self, content, headers, msg):
        """Service operation: Register a "topic" that can be published on and
        that can be subscribed to. Note: this has no direct connection to any
        AMQP topic notion. A topic is basically a data stream.
        """
        
    def op_define_publisher(self, content, headers, msg):
        """Service operation: Register a publisher that subsequently is
        authorized to publish on a topic.
        """

    def op_subscribe(self, content, headers, msg):
        """Service operation: Register a subscriber's intent to receive
        subscriptions on a topic, with additional filter and delivery method
        details.
        """
        subscriber = None
        topic = None
        eventOnly = False

    def op_unsubscribe(self, content, headers, msg):
        """Service operation: Stop one's existing subscription to a topic.
        """

    def op_publish(self, content, headers, msg):
        """Service operation: Publish data message on a topic
        """
        
    def find_topic(self, content, headers, msg):
        """Service operation: For a given resource, find the topic that contains
        updates to the resource or resource description. Might involve creation
        of this topic of this topic does not yet exist
        """
    
# Spawn of the process using the module name
factory = ProtocolFactory(DataPubsubService)
