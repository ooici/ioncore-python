#!/usr/bin/env python

"""
@file ion/services/dm/datapubsub.py
@author Michael Meisinger
@brief service for publishing on data streams, and for subscribing to streams
"""


from twisted.internet import defer
from magnet.store import Store

from ion.core import bootstrap
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class DataPubsubService(BaseService):
    """Data publish/subscribe service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_pubsub', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.topics = Store()

    @defer.inlineCallbacks
    def op_define_topic(self, content, headers, msg):
        """Service operation: Register a "topic" that can be published on and
        that can be subscribed to. Note: this has no direct connection to any
        AMQP topic notion. A topic is basically a data stream.
        """
        topic_name = content['topic_name']
        topic = {topic_name:{'name_type':'fanout', 'args':{'scope':'local'}}}
        yield bootstrap.bs_messaging(topic)
        qtopic_name = self.get_scoped_name('local',topic_name)
        yield self.topics.put (topic_name, topic[topic_name])
        yield self.reply_message(msg, 'result', {'topic_name':qtopic_name}, {})

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

    @defer.inlineCallbacks
    def op_publish(self, content, headers, msg):
        """Service operation: Publish data message on a topic
        """
        topic_name = content['topic_name']
        headers = content['msg_headers']
        op = content['msg_op']
        msg = content['msg']
        qtopic = self.get_scoped_name('local',topic_name)
        # Todo: impersonate message as from sender
        yield self.send_message(qtopic, op, msg, headers)

    def find_topic(self, content, headers, msg):
        """Service operation: For a given resource, find the topic that contains
        updates to the resource or resource description. Might involve creation
        of this topic of this topic does not yet exist
        """

# Spawn of the process using the module name
factory = ProtocolFactory(DataPubsubService)


class DataPubsubClient(BaseServiceClient):
    """Client class for accessing the data pubsub service.
    """
    def __init__(self, *args):
        BaseServiceClient.__init__(self, *args)
        self.svcname = "data_pubsub"

    @defer.inlineCallbacks
    def define_topic(self, topic_name):
        yield self._check_init()
        (content, headers, msg) = yield self.proc.rpc_send(self.svc, 'define_topic', {'topic_name':topic_name}, {})
        defer.returnValue(str(content['topic_name']))

    @defer.inlineCallbacks
    def subscribe(self, topic_name):
        pass
