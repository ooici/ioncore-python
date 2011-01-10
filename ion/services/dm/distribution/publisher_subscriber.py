#!/usr/bin/env python

"""
@file ion/services/dm/distribution/publisher_subscriber.py
@author Paul Hubbard
@author Dave Foster <dfoster@asascience.com>
@brief Publisher/Subscriber classes for attaching to processes
"""

from ion.core.process.process import ProcessClientBase
from ion.core.messaging.receiver import PublisherReceiver, SubscriberReceiver
from ion.services.dm.distribution.pubsub_service import PubSubClient
from twisted.internet import defer

class Publisher(ProcessClientBase):
    """
    @brief This represents publishers of (mostly) science data. Intended use is
    to be instantiated within another class/process/codebase, as an object for sending data to OOI.
    @note All returns are HTTP return codes, 2xx for success, etc, unless otherwise noted.
    """

    def __init__(self, proc=None):
        """
        Save class variables for later
        """
        ProcessClientBase.__init__(self, proc)
        self._resource_id = ''
        self._exchange_point = ''

        self._pubsub_client = PubSubClient(proc=proc)
        self._publish_receiver = PublisherReceiver(self.proc.id.full) # TODO: does it matter?

        # do NOT attach the receiver - don't even init it. We aren't creating queues with it.

    @defer.inlineCallbacks
    def register(self, xp_name, topic_id, publisher_name, credentials):
        """
        @brief Register a new publisher, also does the access control step.
        @param xp_name Name of exchange point to use
        @param topic_id Topic to publish to
        @param publisher_name Name of new publisher process, free-form string
        @param credentials Placeholder for auth* tokens
        @retval OK or error
        @note saves resource id to self._resource_id
        """
        ret = yield self._pubsub_client.define_publisher(xp_name, topic_id, publisher_name, credentials)
        defer.returnValue(ret)

    def unregister(self):
        """
        @brief Remove a registration when done
        @note Uses class variables self._resource_id and self._exchange_point
        @retval Return code only
        """
        pass

    def publish(self, topic, data):
        """
        @brief Publish data on a specified resource id/topic
        @param topic Topic to send to, in form of dataset.variable
        @param data Data, OOI-format, protocol-buffer encoded
        @retval Deferred on send, not RPC
        """
        return self._publish_receiver.send(exchange_point=self._exchange_point, topic=topic, resource_id=self._resource_id, data=data)

# =================================================================================

class PublisherFactory(object):
    """
    A factory class for building Publisher objects.
    """

    def __init__(self, proc=None, xp_name=None, topic_id=None, publisher_name=None, credentials=None):
        """
        Initializer. Sets default properties for calling the build method.

        These default are overridden by specifying the same named keyword arguments to the 
        build method.

        @param  proc        The process the publisher should attach to. May be None to create an
                            anonymous process contained in the Publisher instance itself.
        @param  xp_name     Name of exchange point to use
        @param  topic_id    Topic to publish to
        @param  publisher_name Name of new publisher process, free-form string
        @param  credentials Placeholder for auth* tokens
        """
        self._proc              = proc
        self._xp_name           = xp_name
        self._topic_id          = topic_id
        self._publisher_name    = publisher_name
        self._credentials       = credentials

    @defer.inlineCallbacks
    def build(self, proc=None, xp_name=None, topic_id=None, publisher_name=None, credentials=None):
        """
        Creates a publisher and calls register on it.

        The parameters passed to this method take defaults that were set up when this SubscriberFactory
        was initialized. If None is specified for any of the parameters, or they are not filled out as
        keyword arguments, the defaults take precedence.

        @param  proc        The process the publisher should attach to. May be None to create an
                            anonymous process contained in the Publisher instance itself.
        @param  xp_name     Name of exchange point to use
        @param  topic_id    Topic to publish to
        @param  publisher_name Name of new publisher process, free-form string
        @param  credentials Placeholder for auth* tokens
        """
        proc            = proc or self._proc
        xp_name         = xp_name or self._xp_name
        topic_id        = topic_id or self._topic_id
        publisher_name  = publisher_name or self._publisher_name
        credentials     = credentials or self._credentials

        pub = Publisher(proc)
        yield pub.register(xp_name, topic_id, publisher_name, credentials)

        defer.returnValue(pub)

# =================================================================================

class Subscriber(ProcessClientBase):
    """
    @brief This represents subscribers, both user-driven and internal (e.g. dataset persister)
    @note All returns are HTTP return codes, 2xx for success, etc, unless otherwise noted.
    @todo Need a subscriber receiver that can hook into the topic xchg mechanism
    """
    def __init__(self, proc=None):
        """
        Save class variables for later
        """
        ProcessClientBase.__init__(self, proc)
        self._resource_id = ''
        self._exchange_point = ''

        self._pubsub_client = PubSubClient(proc=proc)
        self._subscribe_receiver = SubscriberReceiver(self.proc.id.full, handler=self._receive_handler) # TODO: use what for name here?

    @defer.inlineCallbacks
    def subscribe(self, xp_name, topic_regex):
        """
        @brief Register a topic to subscribe to. Results will be sent to our private queue.
        @note Allow third-party subscriptions?
        @note Allow multiple subscriptions?
        @param xp_name Name of exchange point to use
        @param topic_regex Dataset topic of interest, using amqp regex
        @retval Return code only, with new queue automagically hooked up to ondata()?
        """
        # TODO: xp_name not taken by PSC?
        self._resource_id = yield self._pubsub_client.subscribe(topic_regex)
        self._subscriber_receiver.name = self._resource_id      # ugh this can't be good
        self._subscriber_receiver.attach()          # "declares" queue in initialize, listens to queue in activate
        defer.returnValue(True)

    def unsubscribe(self):
        """
        @brief Remove a subscription
        @retval Return code only
        """
        self._pubsub_client.unsubscribe(self._resource_id)

    def _receive_handler(self, data, msg):
        """
        Default handler for messages received by the SubscriberReceiver.
        Acks the message and calls the ondata handler.
        @param data Data packet/message matching subscription
        @param msg  Message instance
        @return The return value of ondata.
        """
        msg.ack()
        return self.ondata(data)

    def ondata(self, data):
        """
        @brief Data callback, in the pattern of the current subscriber code
        @param data Data packet/message matching subscription
        @retval None, may daisy chain output back into messaging system
        """
        raise NotImplemented('Must be implmented by subclass')


# =================================================================================

class SubscriberFactory(object):
    """
    Factory to create Subscribers.
    """

    def __init__(self, proc=None, xp_name=None, topic_regex=None, subscriber_type=None):
        """
        Initializer. Sets default properties for calling the build method.

        These default are overridden by specifying the same named keyword arguments to the 
        build method.

        @param  proc        The process the subscriber should attach to. May be None to create an
                            anonymous process contained in the Subscriber instance itself.
        @param  xp_name     Name of exchange point to use
        @param  topic_regex Dataset of topic of interest, using amqp regex
        @param  subscriber_type Specific derived Subscriber type to use. You can define a custom
                            Subscriber derived class if you want to share the implementation
                            across multiple Subscribers. If left None, the standard Subscriber
                            class is used.
        """

        self._proc              = proc
        self._xp_name           = xp_name
        self._topic_regex       = topic_regex
        self._subscriber_type   = subscriber_type

    def build(self, proc=None, xp_name=None, topic_regex=None, subscriber_type=None, handler=None):
        """
        Creates a subscriber.

        The parameters passed to this method take defaults that were set up when this SubscriberFactory
        was initialized. If None is specified for any of the parameters, or they are not filled out as
        keyword arguments, the defaults take precedence.

        @param  proc        The process the subscriber should attach to. May be None to create an
                            anonymous process contained in the Subscriber instance itself.
        @param  xp_name     Name of exchange point to use
        @param  topic_regex Dataset of topic of interest, using amqp regex
        @param  subscriber_type Specific derived Subscriber type to use. You can define a custom
                            Subscriber derived class if you want to share the implementation
                            across multiple Subscribers. If left None, the standard Subscriber
                            class is used.
        @param  handler     A handler method to replace the Subscriber's ondata method. This is typically
                            a bound method of the process owning this Subscriber, but may be any
                            callable taking a data param. If this is left None, the subscriber_type
                            must be set to a derived Subscriber that overrides the ondata method.
        """
        proc            = proc or self._proc
        xp_name         = xp_name or self._xp_name
        topic_regex     = topic_regex or self._topic_regex
        subscriber_type = subscriber_type or self._subscriber_type or Subscriber

        sub = subscriber_type(proc=proc)
        sub.subscribe(xp_name, topic_regex)
        if handler != None:
            sub.ondata = handler

        return sub
