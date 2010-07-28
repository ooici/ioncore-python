#!/usr/bin/env python

"""
@file ion/services/dm/datapubsub.py
@author Michael Meisinger
@brief service for publishing on data streams, and for subscribing to streams
@Note Should the pubsub service use the pubsub registry client or should it
implement the communication with the pubsub registry service?
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer

from ion.core import bootstrap
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.resources import dm_resource_descriptions

from ion.data import dataobject
from ion.services.dm.datapubsub import pubsub_registry

import ion.util.procutils as pu
from ion.core import ioninit
CONF = ioninit.config(__name__)


class DataPubsubService(BaseService):
    """Data publish/subscribe service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_pubsub',
                                          version='0.1.0',
                                          dependencies=[])
    @defer.inlineCallbacks
    def slc_init(self):
        
        # Is this the proper way to start a client in a service?
        self.reg = yield pubsub_registry.DataPubsubRegistryClient(proc=self)
        

    @defer.inlineCallbacks
    def op_define_topic(self, content, headers, msg):
        """Service operation: Register a "topic" that can be published on and
        that can be subscribed to. Note: this has no direct connection to any
        AMQP topic notion. A topic is basically a data stream.
        """
        
        logging.info('msg headers:'+ str(headers))
        topic = dataobject.Resource.decode(content)
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', topic: \n' + str(topic))
  
        yield bootstrap.declare_messaging(topic.queue_properties)
    
        topic = yield self.reg.register(topic)
        if topic:
            yield self.reply_ok(msg, topic.encode())
        else:
            yield self.reply_err(msg, None)

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
        qtopic = self.get_scoped_name('system',topic_name)
        # Todo: impersonate message as from sender
        yield self.send(qtopic, op, msg, headers)

    def find_topic(self, content, headers, msg):
        """Service operation: For a given resource, find the topic that contains
        updates to the resource or resource description. Might involve creation
        of this topic of this topic does not yet exist
        """

# Spawn of the process using the module name
factory = ProtocolFactory(DataPubsubService)


class DataPubsubClient(BaseServiceClient):
    """
    Client class for accessing the data pubsub service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_pubsub"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_topic(self, topic):
        
        assert isinstance(topic, dataobject.Resource), 'Invalid argument to base_register_resource'
        
        # For now assume topic is new!
        topic.set_fanout_topic() # Create a new topic queue identity
        topic.create_new_reference() # Give it a registry identity

        (content, headers, msg) = yield self.rpc_send('define_topic',
                                            topic.encode())
        logging.info('Service reply: '+str(headers))
        if content['status']=='OK':
            topic = dataobject.Resource.decode(content['value'])
            defer.returnValue(topic)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def subscribe(self, topic_name):
        pass
