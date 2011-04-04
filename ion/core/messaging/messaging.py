#!/usr/bin/env python

"""
@author Dorian Raymer
@author Michael Meisinger
@brief AMQP configuration factories as a function of application/service level names.
"""

import uuid

from twisted.internet import defer

from txamqp.client import TwistedDelegate
from txamqp.content import Content

from ion.core.messaging import amqp
from ion.core.messaging import serialization
from ion.core.exception import FatalError
from ion.core.cc.store import Store
from ion.util.state_object import BasicLifecycleObject
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class AMQPEvents(TwistedDelegate):
    """
    This class defines handlers for asynchronous amqp events (events the
    broker can raise at any time).
    """

    def __init__(self, manager):
        TwistedDelegate.__init__(self)
        self.manager = manager

    def basic_deliver(self, ch, msg):
        """
        """
        d = defer.maybeDeferred(ch.deliver_callback, msg)
        d.addErrback(self.manager.delivery_error, msg) #XXX

    def basic_return(self, ch, msg):
        """implement this to handle messages that could not be delivered to
        a queue or consumer
        """
        log.warning("""basic.return event received! This means the broker\
                could not guarantee reliable delivery for a message sent\
                from a process in this container.""")

    def channel_flow(self, ch, msg):
        """implement this to handle a broker flow control request
        """
        log.warning('channel.flow event received')

    def channel_alert(self, ch, msg):
        """implement this to handle a broker channel.alert notification.
        """
        log.warning('channel.alert event received')
        
    def close(self, reason):
        """The AMQClient protocol calls this as a result of a
        connectionLost event. The TwistedDelegate.close method finishes
        shutting off the client, and we get a chance to react to the event
        here.
        """
        TwistedDelegate.close(self, reason)
        self.manager.connectionLost(reason) #XXX notify container that we lost the connection


class MessageSpace(BasicLifecycleObject):
    """
    Represents a connection to a broker vhost with credentials.
    Follows a basic life cycle.
    """

    def __init__(self, exchange_manager, hostname='localhost', port=5672,
            virtual_host='/', heartbeat=0):
        """
        @param exchange_manager So we can link our states. If I'm in a bad
        state, then I need to notify ExchangeManager.
        """
        BasicLifecycleObject.__init__(self)
        self.client = None
        self.exchange_manager = exchange_manager
        self.hostname = hostname
        self.port = port
        self.virtual_host = virtual_host
        self.heartbeat = heartbeat

        # Immediately transition to READY state
        self.initialize()
        self.closing = False # State that determines if we expect a close event

    def on_initialize(self, *args, **kwargs):
        """
        Nothing to do here. What needed to be done was done in __init__
        """
        #self.connection = connection.BrokerConnection(*args, **kwargs)
        #self.closing = False # State that determines if we expect a close event

    def on_activate(self, *args, **kwargs):
        #assert not self.connection._connection, "Already connected to broker"
        from twisted.internet import reactor # This could be the process
                                             # interface...one day 
        amqpEvents = AMQPEvents(self)
        clientCreator = amqp.ConnectionCreator(reactor,
                                    vhost=self.virtual_host,
                                    delegate=amqpEvents,
                                    heartbeat=self.heartbeat)
        d = clientCreator.connectTCP(self.hostname, self.port)
        def connected(client):
            log.info('connected')
            self.client = client
        d.addCallback(connected)
        d.addErrback(self.connectionLost)
        return d

    def on_deactivate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_terminate(self, *args, **kwargs):
        self.closing = True
        return defer.maybeDeferred(self.close_connection)

    def close_connection(self):
        """Close the AMQP broker connection by calling connection_close on
        channel 0"""
        if self.client.closed:
            return
        def connection_close_ok(result):
            self.client.transport.loseConnection()
        chan0 = self.client.channel(0)
        d = chan0.connection_close(reply_code=200, reply_text='Normal Shutdown')
        d.addCallback(connection_close_ok)
        return d

    def on_error(self, *args, **kwargs):
        #raise RuntimeError("Illegal state change for MessageSpace")
        self.exchange_manager.error(*args, **kwargs)

    def delivery_error(self, reason, msg):
        """
        @brief The processing of a message has the ability to raise a
        FatalError exception. This lets the container know that the process
        should be killed. Exceptions other than FatalError are ignored.
        @note A configurable list of Exceptions could be added to this
        class, to enable more types of fatal exceptions.
        """
        log.warning('MessageSpace delivery error')
        log.warning(str(reason))
        if reason.check(FatalError):
            self.exchange_manager.container.fatalError(reason)

    def connectionLost(self, reason):
        """
        AMQP Client triggers this event when it has a conenctionLost event
        itself.
        """
        if not self.closing: #only notify of unexpected closure
            self.exchange_manager.connectionLost(reason) # perpetuate the event

    def __repr__(self):
        params = ['hostname',
                'userid',
                'password',
                'virtual_host',
                ]
        s = "MessageSpace("
        for param in params:
            s += param + "='%s', " % getattr(self.connection, param)
        s += "port=%d" % self.connection.port
        s += ")"
        return s

class ExchangeSpace(object):
    """
    give it a name and a connection
    """
    def __init__(self, message_space, name):
        self.name = name
        self.message_space = message_space
        #self.connection = self.message_space.connection
        self.client = self.message_space.client

        # @todo remove: Store of messaging names
        self.store = Store()

class ProcessExchangeSpace(ExchangeSpace):
    """
    Exchange Space with support for only process participants. Such participants
    can uniquely identified by name. Services and fanout names fall into the
    same category.
    """
    def __init__(self, message_space, name):
        ExchangeSpace.__init__(self, message_space, name)
        self.type = "process"
        self.exchange = Exchange(name)

    @defer.inlineCallbacks
    def send(self, to_name, message_data, publisher_config=None, **kwargs):
        if publisher_config is None: publisher_config = {}
        
        pub_config = {'routing_key' : str(to_name)}
        pub_config.update(publisher_config)
        publisher = yield Publisher.name(self, pub_config)
        yield publisher.send(message_data)
        publisher.close()


class TopicExchangeSpace(ExchangeSpace):
    """
    Exchange Space with support for topic trees (Exchange Points). Such
    participants have a name but
    """

class ExchangeName(object):
    """
    High-level messaging name.
    Encapsulates messaging (amqp) details

    Might also retain name config dict
    OR might just be the config
    """

class Exchange(object):
    """
    Represents an AMQP exchange (name and type) in the context of an
    Exchange Space.

    Currently, the Container has a default space (vhost '/'). An exchange
    point is just a well known exchange processes can send messages
    through. It abstracts away amqp details like exchange type, persistance
    options, etc.
    The Container has a default exchange called 'magnet.topic'. The default
    exchange is a topic exchange; topic is generaly useful and flexible

    The amqp exchange parameters stored in this class only have to do with
    with the amqp method exchange_declare.
    """

    exchange = ''
    exchange_type = 'topic'
    durable = False
    auto_delete = True # deletes when all queues finish (unbind)

    def __init__(self, exchange, **kwargs):
        """
        @param connection the broker connection. It's called space becasue it is
        also a natural namespace, but not yet the one Magnet will use for
        it's "Exchange Space"
        """
        self.exchange = exchange
        self.exchange_type = kwargs.get('exchange_type', self.exchange_type)
        self.durable = kwargs.get('durable', self.durable)
        self.auto_delete = kwargs.get('auto_delete', self.auto_delete)
        self.config_dict = {'exchange':self.exchange,
                            'exchange_type':self.exchange_type,
                            'durable':self.durable,
                            'auto_delete':self.auto_delete,
                            }

##################################################################
## Transition code copied from carrot
ACKNOWLEDGED_STATES = frozenset(["ACK", "REJECTED", "REQUEUED"])


class MessageStateError(Exception):
    """The message has already been acknowledged."""


class Message(object):
    """A message received by the broker.
    This re-presents the amqp basic content message from txamp. This is
    kind of a vestige from carrot. This also adds functionality to the
    received message; actions take place in the context of an amqp channel.
    """

    def __init__(self, channel, amqp_message, **kwargs):
        self.channel = channel
        self._amqp_message = amqp_message
        self.body = amqp_message.content.body
        self.delivery_tag = amqp_message.delivery_tag
        self._decoded_cache = None
        self._state = "RECEIVED"
        for attr_name in (
                          "content type",
                          "content encoding",
                          "headers",
                          "delivery mode",
                          "priority",
                          "correlation id",
                          "reply to",
                          "expiration",
                          "message id",
                          "timestamp",
                          "type",
                          "user id",
                          "app id",
                          "cluster id",
                          ):
            setattr(self, attr_name.replace(' ', '_'),
                            amqp_message.content.properties.get(attr_name, None))
    def decode(self):
        """Deserialize the message body, returning the original
        python structure sent by the publisher."""
        return serialization.decode(self.body, self.content_type,
                                    self.content_encoding)

    @property
    def payload(self):
        """The decoded message."""
        if not self._decoded_cache:
            self._decoded_cache = self.decode()
        return self._decoded_cache

    def ack(self):
        """Acknowledge this message as being processed.,
        This will remove the message from the queue.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        d = self.channel.basic_ack(self.delivery_tag)
        self._state = "ACK"
        return d

    def reject(self):
        """Reject this message.

        The message will be discarded by the server.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        d = self.channel.basic_reject(self.delivery_tag, requeue=False)
        self._state = "REJECTED"
        return d

    def requeue(self):
        """Reject this message and put it back on the queue.

        You must not use this method as a means of selecting messages
        to process.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        d = self.channel.basic_reject(self.delivery_tag, requeue=True)
        self._state = "REQUEUED"
        return d

    @property
    def acknowledged(self):
        return self._state in ACKNOWLEDGED_STATES

##
##############################################################


class Consumer(object):
    """
    Consumer for AMQP.
    """

    def __init__(self, chan, queue=None, 
                             exchange=None, 
                             routing_key=None,
                             exchange_type="direct",
                             durable=False,
                             exclusive=False,
                             auto_delete=True,
                             no_ack=True,
                             binding_key=None,
                             **kwargs): # **kwargs is a sloppy hack
        self.channel = chan
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key
        self.exchange_type = exchange_type
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.no_ack = no_ack
        self.consumer_tag = uuid.uuid4().hex
        self.callback = None
        self._closed = False # Assuming we were given an open channel

    @classmethod
    def new(cls, client, **kwargs):
        """
        Use this creator for deferred instantiation.
        inst.declare returns a deferred that will fire with the Consumer
        instance.
        """
        chan = client.channel()
        d = chan.channel_open()
        def instantiate(result, chan, **kwargs):
            inst = cls(chan, **kwargs)
            return inst.declare()
        d.addCallback(instantiate, chan, **kwargs)
        return d

    @defer.inlineCallbacks
    def declare(self):
        """Declares the queue, the exchange and binds the queue to
        the exchange."""
        arguments = {}
        routing_key = self.routing_key

        # In the current design, exchange is always defined
        if self.exchange:
            yield self.channel.exchange_declare(exchange=self.exchange,
                                          type=self.exchange_type,
                                          durable=self.durable,
                                          auto_delete=self.auto_delete)

        if self.queue:
            # Specific queue name given
            yield self.channel.queue_declare(queue=self.queue,
                                       durable=self.durable,
                                       exclusive=self.exclusive,
                                       auto_delete=self.auto_delete)
        else:
            # Generate internal unique name
            reply = yield self.channel.queue_declare(queue="",
                                       durable=self.durable,
                                       exclusive=self.exclusive,
                                       auto_delete=self.auto_delete)
            # remember the queue name the broker made for us
            self.queue = reply.queue

        if routing_key != None:
            yield self.channel.queue_bind(queue=self.queue,
                                        exchange=self.exchange,
                                        routing_key=routing_key,
                                        arguments=arguments)

        yield self.channel.basic_qos(prefetch_size=0, prefetch_count=1,
                                                        global_=False)

        defer.returnValue(self)

    @classmethod
    def name(cls, ex_space, config):
        """
        @brief configure name with out creating a consumer yet.
        @param ex_space is the broker connection, and (in the current design)
        the exchange information.
        @param config is a dict of amqp options that __init__ extracts.
        """
        client = ex_space.client # amqp client
        full_config = ex_space.exchange.config_dict.copy()
        full_config.update(config)
        chan = client.channel()
        d = chan.channel_open()
        def instantiate(result, chan, **kwargs):
            inst = cls(chan, **kwargs)
            return inst.declare()
        d.addCallback(instantiate, chan, **full_config)
        return d

    def receive(self, amqp_message):
        message = Message(self.channel, amqp_message)
        return self.callback(message) 

    def consume(self, callback, limit=None):
        self.callback = callback
        self.channel.set_consumer_callback(self.receive)
        d = self.channel.basic_consume(queue=self.queue,
                                        no_ack=self.no_ack,
                                        consumer_tag=self.consumer_tag,
                                        nowait=False)
        return d

    def close(self):
        """
        Close the amqp channel, deactivating the Consumer.
        """
        if not self._closed:
            d = self.channel.channel_close()
            self._closed = True
            return d
        return defer.succeed(None)

class Publisher(object):
    """
    Publisher for one message

    delivery_modes:
    1 - non persistent
    2 - persistent 
    """

    def __init__(self, chan, exchange=None, 
                             routing_key=None,
                             exchange_type="direct",
                             delivery_mode=1,
                             durable=False,
                             exclusive=False,
                             auto_delete=True,
                             immediate=False,
                             mandatory=False,
                             **kwargs): # **kwargs is a sloppy hack
        self.channel = chan
        self.exchange = exchange
        self.routing_key = routing_key
        self.exchange_type = exchange_type
        self.delivery_mode = delivery_mode
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.mandatory = mandatory
        self.immediate = immediate
        self._closed = False # Assuming we were given an open channel

    @defer.inlineCallbacks
    def declare(self):

        yield self.channel.exchange_declare(exchange=self.exchange,
                                      type=self.exchange_type,
                                      durable=self.durable,
                                      auto_delete=self.auto_delete)
        defer.returnValue(self)

    @classmethod
    def name(cls, ex_space, config):
        """
        Factory to create new Publisher instance from given params
        """
        if not config:
            raise RuntimeError("Publisher.name(): No config given")
        full_config = ex_space.exchange.config_dict.copy()
        full_config.update(config)

        client = ex_space.client # amqp client
        full_config = ex_space.exchange.config_dict.copy()
        full_config.update(config)
        chan = client.channel()
        d = chan.channel_open()
        # Are we doing an exchange declare on every send???
        def instantiate(result, chan, **kwargs):
            inst = cls(chan, **kwargs)
            return inst.declare()
        d.addCallback(instantiate, chan, **full_config)
        return d

    def create_message(self, message_data, delivery_mode=None, priority=None,
                       content_type=None, content_encoding=None,
                       serializer=None, reply_to=None):
        """With any data, serialize it and encapsulate it in a AMQP
        message with the proper headers set."""

        delivery_mode = delivery_mode or self.delivery_mode

        # No content_type? Then we're serializing the data internally.
        if not content_type:
            (content_type, content_encoding,
             message_data) = serialization.encode(message_data,
                                                  serializer=serializer)
        else:
            # If the programmer doesn't want us to serialize,
            # make sure content_encoding is set.
            if isinstance(message_data, unicode):
                if not content_encoding:
                    content_encoding = 'utf-8'
                message_data = message_data.encode(content_encoding)

            # If they passed in a string, we can't know anything
            # about it.  So assume it's binary data.
            elif not content_encoding:
                content_encoding = 'binary'

        return self.prepare_message(message_data, delivery_mode,
                                            priority=priority,
                                            content_type=content_type,
                                            content_encoding=content_encoding,
                                            reply_to=reply_to)

    def prepare_message(self, message_data, delivery_mode, priority=None,
                              content_type=None, content_encoding=None, headers=None,
                              reply_to=None, correlation_id=None, expiration=None,
                              message_id=None, timestamp=None, type=None, user_id=None,
                              app_id=None, cluster_id=None):
        """Encapsulate data into a AMQP message.
        This method should be reconciled with interceptor functionality and
        the ion message format. 
        """
        if headers is None:
            headers = {}

        properties = {
                  'content type':content_type,
                  'content encoding':content_encoding,
                  'application headers':headers,
                  'delivery mode':delivery_mode,
                  'priority':priority,
                  'correlation id':correlation_id,
                  'reply to':reply_to,
                  'expiration':expiration,
                  'message id':message_id,
                  'timestamp':timestamp,
                  'type':type,
                  'user id':user_id,
                  'app id':app_id,
                  'cluster id':cluster_id,
                  }
        message = Content(message_data, properties=properties)
        return message

    def send(self, message_data, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0, 
                content_type=None, content_encoding=None, reply_to=None, 
                headers=None, serializer=None):
        """Send a message.

        :keyword content_type: The messages content_type. If content_type
            is set, no serialization occurs as it is assumed this is either
            a binary object, or you've done your own serialization.
            Leave blank if using built-in serialization as our library
            properly sets content_type.

        :keyword content_encoding: The character set in which this object
            is encoded. Use "binary" if sending in raw binary objects.
            Leave blank if using built-in serialization as our library
            properly sets content_encoding.

        """
        if headers is None:
            headers = {}
        routing_key = routing_key or self.routing_key

        message = self.create_message(message_data, priority=priority,
                                      delivery_mode=delivery_mode,
                                      content_type=content_type,
                                      content_encoding=content_encoding,
                                      serializer=serializer,
                                      reply_to=reply_to)
        return self.channel.basic_publish(content=message,
                                        exchange=self.exchange, 
                                        routing_key=routing_key,
                                        mandatory=self.mandatory, 
                                        immediate=self.immediate)


    def close(self):
        """
        Close the amqp channel, deactivating the Consumer.
        """
        if not self._closed:
            d = self.channel.channel_close()
            self._closed = True
            return d
        return defer.succeed(None)

def worker(name):

   return {'durable' : False,
           'queue' : name,
           'binding_key' : name,
           'exclusive' : False,
           'mandatory' : True,
           'no_ack' : False,
           'auto_delete' : True,
           'routing_key' : name,
           'immediate' : False,
            }

def direct(name):

   return {'durable' : False,
           'queue' : name,
           'binding_key' : name,
           'exclusive' : True,
           'mandatory' : True,
           'no_ack' : False,
           'auto_delete' : True,
           'routing_key' : name,
           'immediate' : False,
            }

def process(name):
    return direct(name)

def fanout(name):

   return {'durable' : False,
           'queue' : '',
           'binding_key' : name,
           'exclusive' : True,
           'mandatory' : True,
           'no_ack' : False,
           'auto_delete' : True,
           'routing_key' : name,
           'immediate' : False,
            }

#def consume_on(name, config_factory=''):
#    """
#    @param name Name others send messages to (queue name)
#    @param type messaging pattern; Experimental way to configure message system.
#    @brief Experimental mechanism for applications to create more
#    sophisticated message patterns.
#    @retval defer.Deferred that fires a consumer instance
#
#    notes:
#    Create a consumer based on an existing queue/ messaging pattern set up in
#    the broker.
#
#    """
#    amqp_conf = config_factory(name)
#    consumer = messaging.Consumer(ioninit.container_instance.exchange_manager.message_space.connection, **amqp_conf)
#    yield consumer.declare()
