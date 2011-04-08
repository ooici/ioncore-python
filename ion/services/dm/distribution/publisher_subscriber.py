#!/usr/bin/env python

"""
@file ion/services/dm/distribution/publisher_subscriber.py
@author Paul Hubbard
@author Dave Foster <dfoster@asascience.com>
@brief Publisher/Subscriber classes for attaching to processes
"""

from ion.util.state_object import BasicLifecycleObject
from ion.core.messaging.receiver import Receiver, WorkerReceiver
from ion.services.dm.distribution.pubsub_service import PubSubClient, \
    PUBLISHER_TYPE, XS_TYPE, XP_TYPE, TOPIC_TYPE, SUBSCRIBER_TYPE
from ion.core.messaging.message_client import MessageClient

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

#noinspection PyUnusedLocal
class Publisher(BasicLifecycleObject):
    """
    @brief This represents publishers of (mostly) science data. Intended use is
    to be instantiated within another class/process/codebase, as an object for sending data to OOI.
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
        return self._recv.attach() # calls initialize/activate, gets receiver in correct state for publishing

    def register(self, xp_name, topic_name, publisher_name, credentials):
        return _psc_setup_publisher(self, xp_name=xp_name, routing_key=topic_name,
                                    credentials=credentials, publisher_name=publisher_name)

    def publish(self, data, routing_key=None):
        """
        @brief Publish data on a specified resource id/topic
        @param data Data, OOI-format, protocol-buffer encoded
        @param routing_key Routing key to publish data on. Normally the Publisher uses the routing key specified at construction time,
                           but this param may be overriden here.
        @retval Deferred on send, not RPC
        """
        routing_key = routing_key or self._routing_key

        kwargs = { 'recipient' : routing_key,
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

    def __init__(self, xp_name=None, credentials=None, process=None, publisher_type=None):
        """
        Initializer. Sets default properties for calling the build method.

        These default are overridden by specifying the same named keyword arguments to the 
        build method.

        @param  xp_name         Name of exchange point to use
        @param  credentials     Placeholder for auth* tokens
        @param  process         Owning process of the Publisher.
        @param  publisher_type  Specific derived Publisher type to construct. You can define a custom
                                Publisher derived class for any custom behavior. If left None, the standard
                                Publisher class is used.
        """
        self._xp_name           = xp_name
        self._credentials       = credentials
        self._process           = process
        self._publisher_type    = publisher_type

        self._topic_name = None
        self._xs_id = None
        self._xp_id = None
        self._topic_id = None
        self._publisher_id = None

    @defer.inlineCallbacks
    def build(self, routing_key, xp_name=None, credentials=None, process=None, publisher_type=None, *args, **kwargs):
        """
        Creates a publisher and calls register on it.

        The parameters passed to this method take defaults that were set up when this SubscriberFactory
        was initialized. If None is specified for any of the parameters, or they are not filled out as
        keyword arguments, the defaults take precedence.

        @param  routing_key     The AMQP routing key that the Publisher will publish its data to.
        @param  xp_name         Name of exchange point to use
        @param  credentials     Placeholder for auth* tokens
        @param  process         Owning process of the Publisher.
        @param  publisher_type  Specific derived Publisher type to construct. You can define a custom
                                Publisher derived class for any custom behavior. If left None, the standard
                                Publisher class is used.
        """
        xp_name         = xp_name or self._xp_name
        credentials     = credentials or self._credentials
        process         = process or self._process
        topic_name      = routing_key or self._topic_name
        publisher_type  = publisher_type or self._publisher_type or Publisher
        publisher_name  = 'Publisher'

        pub = publisher_type(xp_name=xp_name, routing_key=routing_key, credentials=credentials, process=process, *args, **kwargs)
        yield process.register_life_cycle_object(pub)     # brings the publisher to whatever state the process is in

        # Register does the PSC invocations
        yield pub.register(xp_name, topic_name, publisher_name, credentials)

        defer.returnValue(pub)

# =================================================================================
@defer.inlineCallbacks
def _psc_setup(self, mc, psc, xs_name, xp_name, routing_key):
    """
    Workaround for lack of query in the registry: We need CASref IDs for
    exchange space, exchange point and topic to register as a publisher,
    but there's no way to look those up. PSC has a query, but it's in-memory
    and we can't rely on the keys existing, so Every Damned Time we have
    to (for now) do this sequence of calls to setup the entries and save
    the IDs.
    """
    log.debug('xs')
    msg = yield mc.create_instance(XS_TYPE)

    msg.exchange_space_name = xs_name or 'swapmeet'

    rc = yield psc.declare_exchange_space(msg)
    self._xs_id = rc.id_list[0]

    log.debug('xp')
    msg = yield mc.create_instance(XP_TYPE)
    msg.exchange_point_name = xp_name or 'science_data'
    msg.exchange_space_id = self._xs_id

    rc = yield psc.declare_exchange_point(msg)
    self._xp_id = rc.id_list[0]

    log.debug('topic')
    msg = yield mc.create_instance(TOPIC_TYPE)
    msg.topic_name = routing_key or 'NoTopic'
    msg.exchange_space_id = self._xs_id
    msg.exchange_point_id = self._xp_id

    rc = yield psc.declare_topic(msg)
    self._topic_id = rc.id_list[0]

    log.debug('PFactory PSC calls completed')

@defer.inlineCallbacks
def _psc_setup_publisher(self, xs_name='swapmeet', xp_name='science_data', routing_key=None,
                         credentials=None, publisher_name='Unknown'):

    if not self._process:
        log.error('Cannot initialize message client without process!')
        return

    log.debug('Setting up the PSC and saving IDs...')
    mc = MessageClient(proc=self._process)
    psc = PubSubClient(proc=self._process)

    # Call the common stuff first
    yield _psc_setup(self, mc, psc, xs_name, xp_name, routing_key)

    # Register the publisher and save the casref
    msg = yield mc.create_instance(PUBLISHER_TYPE)
    msg.exchange_space_id = self._xs_id
    msg.exchange_point_id = self._xp_id
    msg.topic_id = self._topic_id
    msg.publisher_name = publisher_name
    if credentials:
        msg.credentials = credentials

    log.debug('declaring publisher')
    rc = yield psc.declare_publisher(msg)
    self._publisher_id = rc.id_list[0]

    log.debug('done setting up publisher')

@defer.inlineCallbacks
def _psc_setup_subscriber(self, xs_name='swapmeet', xp_name='science_data',
                          binding_key=None, subscriber_name='Unknown', credentials=None):

    log.debug('Setting up the PSC...')
    if not self._process:
        log.error('Cannot initialize message client without process!')
        return

    mc = MessageClient(proc=self._process)
    psc = PubSubClient(proc=self._process)

    yield _psc_setup(self, mc, psc, xs_name, xp_name, binding_key)
    
    # Subscriber
    # @note Not using credentials yet
    if credentials:
        log.debug('Sorry, ignoring the credentials!')

    log.debug('Subscriber')
    msg = yield mc.create_instance(SUBSCRIBER_TYPE)
    msg.exchange_space_id = self._xs_id
    msg.exchange_point_id = self._xp_id
    msg.topic_id = self._topic_id
    msg.subscriber_name = subscriber_name
    if self._queue_name:
        msg.queue_name = self._queue_name
    rc = yield psc.subscribe(msg)
    self._subscriber_id = rc.id_list[0]

class Subscriber(BasicLifecycleObject):
    """
    @brief This represents subscribers, both user-driven and internal (e.g. dataset persister)
    @note All returns are HTTP return codes, 2xx for success, etc, unless otherwise noted.
    @todo Need a subscriber receiver that can hook into the topic xchg mechanism
    """

    def __init__(self, xp_name=None, binding_key=None, queue_name=None, credentials=None, process=None, *args, **kwargs):

        BasicLifecycleObject.__init__(self)

        assert xp_name and process

        self._xp_name       = xp_name
        self._binding_key   = binding_key
        self._queue_name    = queue_name
        self._credentials   = credentials
        self._process       = process
        self._subscriber_id = None
        self._resource_id = None

        self._pubsub_client = PubSubClient(process=process)

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
        name = process.id.full + "_subscriber_recv_" + \
               str(len(process._registered_life_cycle_objects))
        self._recv = WorkerReceiver(name, process=process,
                                    handler=self._receive_handler,
                                    consumer_config=consumer_config)


    def on_initialize(self, *args, **kwargs):
        pass

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
       yield self.subscribe()

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        yield self._recv.terminate()

    @defer.inlineCallbacks
    def register(self):
        """
        Registers this Subscriber with the PSC.
        Call this prior to calling initialize, as default initialize will try to create a queue.
        """
        yield _psc_setup_subscriber(self, xp_name=self._xp_name, binding_key=self._binding_key)

    @defer.inlineCallbacks
    def subscribe(self):
        """
        """
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

    #noinspection PyUnusedLocal
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

    def __init__(self, xp_name=None, binding_key=None, queue_name=None, subscriber_type=None, process=None, credentials=None):
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
        @param  process     Process that Subscribers will be attached to.
        """

        self._xp_name           = xp_name
        self._binding_key       = binding_key
        self._queue_name        = queue_name
        self._subscriber_type   = subscriber_type
        self._process           = process
        self._credentials       = credentials

    @defer.inlineCallbacks
    def build(self, xp_name=None, binding_key=None, queue_name=None, handler=None, subscriber_type=None, process=None, credentials=None, *args, **kwargs):
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
        @param  process     Process that Subscribers will be attached to.
        @param  credentials Subscriber credentials (not currently used).
        """
        xp_name         = xp_name or self._xp_name
        binding_key     = binding_key or self._binding_key
        queue_name      = queue_name or self._queue_name
        subscriber_type = subscriber_type or self._subscriber_type or Subscriber
        process         = process or self._process
        credentials     = credentials or self._credentials

        sub = subscriber_type(xp_name=xp_name, binding_key=binding_key, queue_name=queue_name,
                              process=process, credentials=credentials, *args, **kwargs)
        yield sub.register()
        # brings the subscriber up to the same state as the process
        yield process.register_life_cycle_object(sub)

        if handler:
            sub.ondata = handler

        defer.returnValue(sub)

