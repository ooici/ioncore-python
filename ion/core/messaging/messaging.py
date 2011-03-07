#!/usr/bin/env python

"""
@author Dorian Raymer
@author Michael Meisinger
@brief AMQP configuration factories as a function of application/service level names.
"""

from twisted.internet import defer

from carrot import connection
from carrot import messaging
from txamqp.client import TwistedDelegate

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
        """notify container when an error occurs during message delivery

        ch._deliver comes from carrot.backends.txamqplib. Delivered
        messages are routed to their endpoint receiver via the amqp channel
        that the receivers consumer was created with.
        This delivery plumbing is definitely messy and hacky. We used to
        use a TwistedDelegate subclass called
        carrot.backends.txamqplib.InterceptionPoint, where the ch._deliver
        made slightly more sense. 
        """
        d = defer.maybeDeferred(ch._deliver, msg)
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

    def __init__(self, exchange_manager, *args, **kwargs):
        """
        @param exchange_manager So we can link our states. If I'm in a bad
        state, then I need to notify ExchangeManager.
        """
        BasicLifecycleObject.__init__(self)
        self.connection = None
        self.exchange_manager = exchange_manager

        # Immediately transition to READY state
        self.initialize(*args, **kwargs)
        self.closing = False # State that determines if we expect a close event

    def on_initialize(self, *args, **kwargs):
        """
        Initializes a MessageSpace analogous to a Carrot BrokerConnection
        instance.
        """
        self.connection = connection.BrokerConnection(*args, **kwargs)
        self.closing = False # State that determines if we expect a close event

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        assert not self.connection._connection, "Already connected to broker"
        amqpEvents = AMQPEvents(self)
        yield self.connection.connect(amqpEvents)

    def on_deactivate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_terminate(self, *args, **kwargs):
        self.closing = True
        return defer.maybeDeferred(self.connection.close)

    def on_error(self, *args, **kwargs):
        #raise RuntimeError("Illegal state change for MessageSpace")
        self.exchange_manager.error(*args, **kwargs)

    def delivery_error(self, msg):
        """
        @param msg Message that resulted in a delivery error
        @notes Is this always a fatal error?
        """

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
        self.connection = self.message_space.connection

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
    def send(self, to_name, message_data, publisher_config={}, **kwargs):
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

class Consumer(messaging.Consumer):
    """
    Dumb Consumer only knows how to consume off an existing queue. It does
    not attempt to do amqp configuration.
    """

    @classmethod
    def new(cls, space, **kwargs):
        """
        Use this creator for deferred instantiation.
        inst.declare returns a deferred that will fire with the Consumer
        instance.
        """
        inst = cls(space, **kwargs)
        return inst.declare()

    @defer.inlineCallbacks
    def declare(self):
        """Declares the queue, the exchange and binds the queue to
        the exchange."""
        arguments = {}
        routing_key = self.routing_key

        # In the current design, exchange is always defined
        if self.exchange:
            yield self.backend.exchange_declare(exchange=self.exchange,
                                          type=self.exchange_type,
                                          durable=self.durable,
                                          auto_delete=self.auto_delete)

        if self.queue:
            # Specific queue name given
            yield self.backend.queue_declare(queue=self.queue,
                                       durable=self.durable,
                                       exclusive=self.exclusive,
                                       auto_delete=self.auto_delete,
                                       warn_if_exists=self.warn_if_exists)
        else:
            # Generate internal unique name
            reply = yield self.backend.queue_declare(queue="",
                                       durable=self.durable,
                                       exclusive=self.exclusive,
                                       auto_delete=self.auto_delete,
                                       warn_if_exists=self.warn_if_exists)
            # remember the queue name the broker made for us
            self.queue = reply.queue


        yield self.backend.queue_bind(queue=self.queue,
                                    exchange=self.exchange,
                                    routing_key=routing_key,
                                    arguments=arguments)

        yield self.qos(prefetch_count=1)

        self._closed = False
        defer.returnValue(self)

    @classmethod
    def name(cls, ex_space, config):
        """
        @brief configure name with out creating a consumer yet.
        @param ex_space is the broker connection, and (in the current design)
        the exchange information.
        @param config is a dict of amqp options that __init__ extracts.
        """
        connection = ex_space.connection # broker connection
        full_config = ex_space.exchange.config_dict.copy()
        full_config.update(config)
        inst = cls(connection, **full_config)
        return inst.declare()

class Publisher(messaging.Publisher):
    """
    Publisher for one message
    """

    @defer.inlineCallbacks
    def declare(self):

        yield self.backend.exchange_declare(exchange=self.exchange,
                                      type=self.exchange_type,
                                      durable=self.durable,
                                      auto_delete=self.auto_delete)
        defer.returnValue(self)

    @classmethod
    def name(cls, ex_space, config):
        """
        Factory to create new Publisher instance from given params
        """
        connection = ex_space.connection # broker connection
        if not config:
            raise RuntimeError("Publisher.name(): No config given")

        full_config = ex_space.exchange.config_dict.copy()
        full_config.update(config)
        inst = cls(connection, **full_config)

        # Are we doing an exchange declare on every send???
        return inst.declare()

def worker(name):

   return {'durable' : False,
           'queue' : name,
           'binding_key' : name,
           'exclusive' : False,
           'mandatory' : True,
           'warn_if_exists' : True,
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
           'warn_if_exists' : True,
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
           'warn_if_exists' : True,
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
