#!/usr/bin/env python

"""
@file ion/services/dm/distribution/publisher_subscriber.py
@author Paul Hubbard
@author Dave Foster <dfoster@asascience.com>
@brief Publisher/Subscriber classes for attaching to processes
"""

from ion.util.state_object import BasicLifecycleObject
from ion.core.messaging.receiver import Receiver, WorkerReceiver
from twisted.internet import defer

from ion.util import procutils as pu

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
CONF = ioninit.config(__name__)

class PSCRegisterable(object):
    """
    An object that is registerable with the PubSubController.

    Provides a mixin-like baseclass to multiple inherit from.  Done so because PSC registration is optional and more
    of a bolt-on style of operation.  This baseclass contains the common setup code, the Publisher and Subscriber
    override as appropriate.

    The initializer of this class does the import of things that may not succeed due to cyclical dependencies - import
    this too early and it will fail etc. It handles all of this and sets a "_pcsr_import_ok" attr on the class to
    indicate success of registration.
    """

    def __init__(self, proc):
        self._pscr_proc = proc
        self._pscr_import_ok = False

        try:
            from ion.core.messaging.message_client import MessageClient
            from ion.services.dm.distribution.pubsub_service import PubSubClient, PUBLISHER_TYPE, XS_TYPE, XP_TYPE, TOPIC_TYPE, SUBSCRIBER_TYPE

            self._pscr_mc = MessageClient(proc=proc)
            self._pscr_pscc = PubSubClient(proc=proc)

            # save these imports as locals - so we don't have to do another protected import
            self._pscr_PUBLISHER_TYPE = PUBLISHER_TYPE
            self._pscr_SUBSCRIBER_TYPE = SUBSCRIBER_TYPE
            self._pscr_XS_TYPE = XS_TYPE
            self._pscr_XP_TYPE = XP_TYPE
            self._pscr_TOPIC_TYPE = TOPIC_TYPE

            # flag
            self._pscr_import_ok = True

        except ImportError:
            log.warn("Could not import MC or PSC in PSCRegisterable base, likely a circular reference")

    @defer.inlineCallbacks
    def psc_setup(self, xs_name=None, xp_name='science_data', routing_key=None,
                         credentials=None, **kwargs):
        '''
        Common PSC setup methods shared between pub/sub derived implementations.
        Call this baseclass method first.

        @returns    A boolean indicating success. Will only return false if the PSCRegisterable
                    was not setup correctly at startup, which indicates an import error and a cyclical
                    import situation. In that case, PSC registration will not be possible.
        '''

        if not self._pscr_import_ok:
            log.warn("Trying to psc_setup when _pscr_import_ok not true!")
            defer.returnValue(False)

        log.debug('xs')
        msg = yield self._pscr_mc.create_instance(self._pscr_XS_TYPE)

        msg.exchange_space_name = xs_name or pu.get_scoped_name('swapmeet', 'system')

        rc = yield self._pscr_pscc.declare_exchange_space(msg)
        self._xs_id = rc.id_list[0]

        log.debug('xp')
        msg = yield self._pscr_mc.create_instance(self._pscr_XP_TYPE)
        msg.exchange_point_name = xp_name or 'science_data'
        msg.exchange_space_id = self._xs_id

        rc = yield self._pscr_pscc.declare_exchange_point(msg)
        self._xp_id = rc.id_list[0]

        log.debug('topic')
        msg = yield self._pscr_mc.create_instance(self._pscr_TOPIC_TYPE)
        msg.topic_name = routing_key or 'NoTopic'
        msg.exchange_space_id = self._xs_id
        msg.exchange_point_id = self._xp_id

        rc = yield self._pscr_pscc.declare_topic(msg)
        self._topic_id = rc.id_list[0]

        log.debug('PFactory PSC calls completed')

        defer.returnValue(True)


#noinspection PyUnusedLocal
class Publisher(BasicLifecycleObject, PSCRegisterable):
    """
    @brief This represents publishers of (mostly) science data. Intended use is
    to be instantiated within another class/process/codebase, as an object for sending data to OOI.
    """

    def __init__(self, xp_name=None, routing_key=None, credentials=None, process=None, *args, **kwargs):
        """
        Initializer for a Publisher.

        @param  xp_name     Exchange Point name.
        @param  routing_key The routing key this publisher will use. May be None or overridden when calling
                            publish.
        @param  credentials Credentials to use.
        @param  process     The owning process of this Publisher. Must be specified.
        """
        BasicLifecycleObject.__init__(self)

        assert xp_name and routing_key and process

        PSCRegisterable.__init__(self, process)

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

        # monkey patch receiver as we don't want any of its initialize or activate items running, but we want it to be in the right state
        def noop(*args, **kwargs):
            pass

        self._recv.on_initialize = noop
        self._recv.on_activate = noop

    def on_initialize(self, *args, **kwargs):
        return self._recv.initialize()      # callback is a no-op but sets correct state

    def on_activate(self, *args, **kwargs):
        return self._recv.activate()        # callback is a no-op but sets correct state

    def on_terminate(self, *args, **kwargs):
        pass

    def register(self, xp_name, topic_name, publisher_name, credentials):
        return self.psc_setup(xp_name=xp_name, routing_key=topic_name, credentials=credentials, publisher_name=publisher_name)

    @defer.inlineCallbacks
    def psc_setup(self, publisher_name=None, **kwargs):

        # call base class boilerplate
        success = yield PSCRegisterable.psc_setup(self, **kwargs)
        if not success:
            log.warn("Publisher could not register with PSC")
            defer.returnValue(False)

        # Register the publisher and save the casref
        msg = yield self._pscr_mc.create_instance(self._pscr_PUBLISHER_TYPE)
        msg.exchange_space_id = self._xs_id # set via parent psc_setup call
        msg.exchange_point_id = self._xp_id # also wik
        msg.topic_id = self._topic_id       # also also wik
        msg.publisher_name = publisher_name
        if kwargs.get('credentials', None):
            msg.credentials = kwargs['credentials']

        log.debug('declaring publisher')
        rc = yield self._pscr_pscc.declare_publisher(msg)
        self._publisher_id = rc.id_list[0]
        log.debug('done setting up publisher')

    def publish(self, data, routing_key=None):
        """
        @brief Publish data on a specified resource id/topic
        @param data Data, OOI-format, protocol-buffer encoded
        @param routing_key Routing key to publish data on. Normally the Publisher uses the routing key specified at construction time,
                           but this param may be overriden here.
        @retval Deferred on send, not RPC
        """
        routing_key = routing_key or self._routing_key

        # set up the sender/sender-name to make it look as if the owning process is doing the sending, which at some level it
        # technically is.
        kwargs = { 'recipient' : routing_key,
                   'content'   : data,
                   'headers'   : {'sender-name' : self._process.proc_name },
                   'operation' : None,
                   'sender'    : self._process.id.full }

        return self._recv.send(**kwargs)

# =================================================================================

class PublisherFactory(object):
    """
    A factory class for building Publisher objects.
    """

    def __init__(self, routing_key=None, xp_name=None, credentials=None, process=None, publisher_type=None):
        """
        Initializer. Sets default properties for calling the build method.

        These default are overridden by specifying the same named keyword arguments to the 
        build method.

        @param  routing_key     Name of the routing key to publish messages on. Typically not set as a factory-wide
                                setting.
        @param  xp_name         Name of exchange point to use
        @param  credentials     Placeholder for auth* tokens
        @param  process         Owning process of the Publisher.
        @param  publisher_type  Specific derived Publisher type to construct. You can define a custom
                                Publisher derived class for any custom behavior. If left None, the standard
                                Publisher class is used.
        """
        self._routing_key       = routing_key
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
    def build(self, routing_key=None, xp_name=None, credentials=None, process=None, publisher_type=None, *args, **kwargs):
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
        routing_key     = routing_key or self._routing_key
        xp_name         = xp_name or self._xp_name
        credentials     = credentials or self._credentials
        process         = process or self._process
        topic_name      = routing_key or self._topic_name
        publisher_type  = publisher_type or self._publisher_type or Publisher
        publisher_name  = 'Publisher'

        pub = publisher_type(xp_name=xp_name, routing_key=routing_key, credentials=credentials, process=process, *args, **kwargs)
        yield process.register_life_cycle_object(pub)     # brings the publisher to whatever state the process is in

        # OOIION-4: making automatic registration of publishers optional due to speed
        if CONF.getValue('register_on_build', False):
            # Register does the PSC invocations
            yield pub.register(xp_name, topic_name, publisher_name, credentials)

        defer.returnValue(pub)


class Subscriber(BasicLifecycleObject, PSCRegisterable):
    """
    @brief This represents subscribers, both user-driven and internal (e.g. dataset persister)
    @note All returns are HTTP return codes, 2xx for success, etc, unless otherwise noted.
    @todo Need a subscriber receiver that can hook into the topic xchg mechanism
    """

    def __init__(self, xp_name=None, binding_key=None, queue_name=None, credentials=None, process=None, durable=False, auto_delete=True, *args, **kwargs):
        """
        Initializer for Subscribers.

        @param  xp_name     Exchange Point name.
        @param  binding_key The binding key (AMQP) to attach this Subscriber to. Can be left blank if attaching to
                            an existing queue.
        @param  queue_name  Queue name to attach to. If the queue exists, attaches to it. If not, it is created. If
                            left blank, an anonymous queue is created.
        @param  credentials Credentials for the Subscriber.
        @param  process     Owning process of this Subscriber. Must be specified.
        @param  durable     If the queue is durable (AMQP), which means the broker will recreate the queue if it
                            restarts. This should not be specified with an anonymous queue name.
        @param  auto_delete If the queue has the auto_delete setting (AMQP), which means that on last consumer detatch
                            from that queue, the queue is deleted.
        """
        BasicLifecycleObject.__init__(self)

        assert xp_name and process

        PSCRegisterable.__init__(self, process)

        # sanity check: worth a warning but not an assert
        if (durable or not auto_delete) and queue_name is None:
            log.warn("Subscriber() - specified no queue name with durable %s or auto_delete %s" % (str(durable), str(auto_delete)))

        self._xp_name       = xp_name
        self._binding_key   = binding_key
        self._queue_name    = queue_name
        self._credentials   = credentials
        self._process       = process
        self._durable       = durable
        self._auto_delete   = auto_delete
        self._subscriber_id = None
        self._resource_id = None

        # set up comms details
        consumer_config = { 'exchange' : self._xp_name,
                            'exchange_type' : 'topic',  # TODO
                            'durable': durable,
                            'auto_delete': auto_delete,
                            'mandatory': True,
                            'immediate': False,
                            'warn_if_exists': False,
                            'routing_key' : self._binding_key,      # may be None, if so, no binding is made to the queue (routing_key is incorrectly named in the dict used by Receiver)
                            'queue' : self._queue_name,              # may be None, if so, the queue is made anonymously (and stored in receiver's consumer.queue attr)
                          }

        # TODO: name?
        self._sub_name = process.id.full + "_subscriber_recv_" + \
                            str(len(process._registered_life_cycle_objects))
        self._recv = WorkerReceiver(self._sub_name, process=process,
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
        yield self.psc_setup(xp_name=self._xp_name, binding_key=self._binding_key, subscriber_name=self._sub_name)

    @defer.inlineCallbacks
    def psc_setup(self, subscriber_name=None, **kwargs):

        # call base class boilerplate
        success = yield PSCRegisterable.psc_setup(self, **kwargs)
        if not success:
            log.warn("Subscriber could not register with PSC")
            defer.returnValue(False)

        # Subscriber
        # @note Not using credentials yet
        if kwargs.get('credentials', None):
            log.debug('Sorry, ignoring the credentials!')

        log.debug('Subscriber')
        msg = yield self._pscr_mc.create_instance(self._pscr_SUBSCRIBER_TYPE)
        msg.exchange_space_id = self._xs_id # set in baseclass psc_setup
        msg.exchange_point_id = self._xp_id # same
        msg.topic_id = self._topic_id       # ""
        msg.subscriber_name = subscriber_name

        if self._queue_name:
            msg.queue_name = self._queue_name

        rc = yield self._pscr_pscc.subscribe(msg)
        self._subscriber_id = rc.id_list[0]

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
        log.warn("Subscriber unsubscribe not implemented")
        # @TODO: is this even used anywhere?
        #yield self._pscr_pscc.unsubscribe(self._resource_id)

    @defer.inlineCallbacks
    def _receive_handler(self, data, msg):
        """
        Default handler for messages received by the SubscriberReceiver.
        Acks the message and calls the ondata handler.
        @param data Data packet/message matching subscription
        @param msg  Message instance
        @return The return value of ondata.
        """
        try:
            ret = yield defer.maybeDeferred(self.ondata, data)
            defer.returnValue(ret)
        finally:
            yield msg.ack()

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

        # OOIION-4: making automatic registration of subscribers optional due to access speed
        if CONF.getValue('register_on_build', False):
            yield sub.register()

        # brings the subscriber up to the same state as the process
        yield process.register_life_cycle_object(sub)

        if handler:
            sub.ondata = handler

        defer.returnValue(sub)

