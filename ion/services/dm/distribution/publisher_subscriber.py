#!/usr/bin/env python

"""
@file ion/services/dm/distribution/publisher_subscriber.py
@author Paul Hubbard
@author Dave Foster <dfoster@asascience.com>
@brief Publisher/Subscriber classes for attaching to processes
"""

from ion.util.state_object import BasicLifecycleObject
from ion.core.process.process import Process
from ion.core.messaging.receiver import Receiver, WorkerReceiver
from ion.services.dm.distribution.pubsub_service import PubSubClient
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class Publisher(BasicLifecycleObject):
    """
    @brief This represents publishers of (mostly) science data. Intended use is
    to be instantiated within another class/process/codebase, as an object for sending data to OOI.
    @note All returns are HTTP return codes, 2xx for success, etc, unless otherwise noted.
    """

    def __init__(self, xp_name=None, routing_key=None, credentials=None, process=None, *args, **kwargs):

        BasicLifecycleObject.__init__(self)

        assert xp_name and routing_key and process

        self._xp_name = xp_name
        self._routing_key = routing_key
        self._credentials = credentials
        self._process = process

        # TODO: will the user specify this? will the PSC get it?
        publisher_config = { 'exchange'      : xp_name,
                             'exchange_type' : 'topic',
                             'durable': False,
                             'mandatory': True,
                             'immediate': False,
                             'warn_if_exists': False }

        # we use base Receiver here as we only send with it, no consumption which the base Receiver doesn't do well
        self._recv = Receiver(routing_key, process=process, publisher_config=publisher_config)
        self._pubsub_client = PubSubClient(process=process)

        # monkey patch receiver as we don't want any of its initialize or activate items running, but we want it to be in the right state
        def noop(*args, **kwargs):
            pass

        self._recv.on_initialize = noop
        self._recv.on_activate = noop

    def on_initialize(self, *args, **kwargs):
        pass

    def on_activate(self, *args, **kwargs):
        self._recv.attach() # calls initialize/activate, gets receiver in correct state for publishing

    def publish(self, data):
        """
        @brief Publish data on a specified resource id/topic
        @param data Data, OOI-format, protocol-buffer encoded
        @retval Deferred on send, not RPC
        """
        kwargs = { 'recipient' : self._routing_key,
                   'content'   : data,
                   'headers'   : {},
                   'operation' : None } #,
                   #'sender'    : self.xname }

        return self._recv.send(**kwargs)

# =================================================================================

class PublisherFactory(object):
    """
    A factory class for building Publisher objects.
    """

    def __init__(self, xp_name=None, credentials=None, process=None):
        """
        Initializer. Sets default properties for calling the build method.

        These default are overridden by specifying the same named keyword arguments to the 
        build method.

        @param  xp_name     Name of exchange point to use
        @param  credentials Placeholder for auth* tokens
        @param  process     Owning process of the Publisher.
        """
        self._xp_name           = xp_name
        self._credentials       = credentials
        self._process           = process

    @defer.inlineCallbacks
    def build(self, routing_key, xp_name=None, credentials=None, process=None):
        """
        Creates a publisher and calls register on it.

        The parameters passed to this method take defaults that were set up when this SubscriberFactory
        was initialized. If None is specified for any of the parameters, or they are not filled out as
        keyword arguments, the defaults take precedence.

        @param  routing_key The AMQP routing key that the Publisher will publish its data to.
        @param  xp_name     Name of exchange point to use
        @param  credentials Placeholder for auth* tokens
        @param  process     Owning process of the Publisher.
        """
        xp_name         = xp_name or self._xp_name
        credentials     = credentials or self._credentials
        process         = process or self._process

        pub = Publisher(xp_name=xp_name, routing_key=routing_key, credentials=credentials, process=process)
        yield process.register_life_cycle_object(pub)     # brings the publisher to whatever state the process is in
        #yield pub.register(xp_name, topic_id, publisher_name, credentials)

        defer.returnValue(pub)

# =================================================================================

class Subscriber(Process):
    """
    @brief This represents subscribers, both user-driven and internal (e.g. dataset persister)
    @note All returns are HTTP return codes, 2xx for success, etc, unless otherwise noted.
    @todo Need a subscriber receiver that can hook into the topic xchg mechanism
    """

    def __init__(self, xp_name=None, binding_key=None, queue_name=None, credentials=None, *args, **kwargs):

        Process.__init__(self, *args, **kwargs)

        assert xp_name

        self._xp_name       = xp_name
        self._binding_key   = binding_key
        self._queue_name    = queue_name
        self._credentials   = credentials

        self._pubsub_client = PubSubClient()

        # set up comms details
        consumer_config = { 'exchange' : self._xp_name,
                            'exchange_type' : 'topic',  # TODO
                            'durable': False,
                            'mandatory': True,
                            'immediate': False,
                            'warn_if_exists': False,
                            'routing_key' : self._binding_key,      # may be None, if so, no binding is made to the queue (routing_key is incorrectly named in the dict used by Receiver)
                            'queue' : self._queue_name,              # may be None, if so, the queue is made anonymously (and stored in receiver's consumer.queue attr)
                          }

        # TODO: name?
        self._recv = WorkerReceiver(self.id.full + "_recv", process=self, handler=self._receive_handler, consumer_config=consumer_config)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
       yield self.subscribe()

    @defer.inlineCallbacks
    def subscribe(self):
        """
        """
        # TODO: PSC interaction?
        yield self._recv.attach()

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
        raise NotImplementedError('Must be implemented by subclass')


# =================================================================================

class SubscriberFactory(object):
    """
    Factory to create Subscribers.
    """

    def __init__(self, xp_name=None, binding_key=None, queue_name=None, subscriber_type=None, credentials=None):
        """
        Initializer. Sets default properties for calling the build method.

        These default are overridden by specifying the same named keyword arguments to the 
        build method.

        @param  xp_name     Name of exchange point to use
        @param  binding_key The binding key to use for the Subscriber. If specified, the queue will have this binding
                            key bound to it.
        @param  queue_name  The queue name to use for the Subscriber. If specified, the queue may either exist or be
                            created. If not specified, an anonymous queue is created.
        @param  subscriber_type Specific derived Subscriber type to use. You can define a custom
                            Subscriber derived class if you want to share the implementation
                            across multiple Subscribers. If left None, the standard Subscriber
                            class is used.
        """

        self._xp_name           = xp_name
        self._binding_key       = binding_key
        self._queue_name        = queue_name
        self._subscriber_type   = subscriber_type
        self._credentials       = credentials

    @defer.inlineCallbacks
    def build(self, xp_name=None, binding_key=None, queue_name=None, handler=None, subscriber_type=Subscriber, credentials=None):
        """
        Creates a subscriber.

        The parameters passed to this method take defaults that were set up when this SubscriberFactory
        was initialized. If None is specified for any of the parameters, or they are not filled out as
        keyword arguments, the defaults take precedence.

        @param  proc        The process the subscriber should attach to. May be None to create an
                            anonymous process contained in the Subscriber instance itself.
        @param  xp_name     Name of exchange point to use
        @param  binding_key The binding key to use for the Subscriber. If specified, the queue will have this binding
                            key bound to it.
        @param  queue_name  The queue name to use for the Subscriber. If specified, the queue may either exist or be
                            created. If not specified, an anonymous queue is created.
        @param  subscriber_type Specific derived Subscriber type to use. You can define a custom
                            Subscriber derived class if you want to share the implementation
                            across multiple Subscribers. If left None, the standard Subscriber
                            class is used.
        @param  handler     A handler method to replace the Subscriber's ondata method. This is typically
                            a bound method of the process owning this Subscriber, but may be any
                            callable taking a data param. If this is left None, the subscriber_type
                            must be set to a derived Subscriber that overrides the ondata method.
        @param  credenitials Subscriber credentials (not currently used).
        """
        xp_name         = xp_name or self._xp_name
        binding_key     = binding_key or self._binding_key
        queue_name      = queue_name or self._queue_name
        subscriber_type = subscriber_type or self._subscriber_type or Subscriber
        credentials     = credentials or self._credentials

        sub = subscriber_type(xp_name=xp_name, binding_key=binding_key, queue_name=queue_name, credentials=credentials)
        yield sub.spawn()

        if handler != None:
            sub.ondata = handler

        defer.returnValue(sub)

