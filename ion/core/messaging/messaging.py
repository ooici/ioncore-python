"""
@author Dorian Raymer
@author Michael Meisinger
@brief AMQP configuration factories as a function of application/service level names.
"""

from twisted.internet import defer

from carrot import connection
from carrot import messaging

class MessageSpace(connection.BrokerConnection):
    """
    Configuration for a broker conection and vhost.
    """

    def __repr__(self):
        params = ['hostname',
                'userid',
                'password',
                'virtual_host',
                ]
        s = "MessageSpace("
        for param in params:
            s += param + "='%s', " % getattr(self, param)
        s += "port=%d" % self.port
        s += ")"
        return s

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

    def __init__(self, connection, exchange, **kwargs):
        """
        @param connection the broker connection. It's called space becasue it is
        also a natural namespace, but not yet the one Magnet will use for
        it's "Exchange Space"
        """
        self.connection = connection
        self.exchange = exchange
        self.exchange_type = kwargs.get('exchange_type', self.exchange_type)
        self.durable = kwargs.get('durable', self.durable)
        self.auto_delete = kwargs.get('auto_delete', self.auto_delete)
        self.config_dict = {'exchange':self.exchange,
                            'exchange_type':self.exchange_type,
                            'durable':self.durable,
                            'auto_delete':self.auto_delete,
                            }

class ProcessExchange(Exchange):
    pass

class TopicExchange(Exchange):
    pass

container_exchange = {
                        'exchange':'container',
                        'exchange_type':'topic',
                        }

def worker(name):

   return {'durable' : False,
           'queue' : name,
           'binding_key' : name,
           'exclusive' : False,
           'mandatory' : True,
           'warn_if_exists' : True,
           'no_ack' : False,
           'auto_delete' : False,
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


class BaseConsumer(messaging.Consumer):
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
        """
        connection = ex_space.connection # broker connection
        full_config = config.copy()
        full_config.update(ex_space.config_dict)
        inst = cls(connection, **full_config)
        return inst.declare()

    @classmethod
    def name(cls, ex_space, config):
        """
        @brief configure name with out creating a consumer yet.
        @param ex_space is the broker connection, and (in the current design)
        the exchange information.
        @param config is a dict of amqp options that __init__ extracts.
        """
        connection = ex_space.connection # broker connection
        full_config = config.copy()
        full_config.update(ex_space.config_dict)
        inst = cls(connection, **full_config)
        return inst.declare()

class Publisher(messaging.Publisher):
    """
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

        full_config = config.copy()
        full_config.update(ex_space.config_dict)
        inst = cls(connection, **full_config)
        # Are we doing an exchange declare on every send???
        return inst.declare()

def consume_on(name, config_factory=''):
    """
    @param name Name others send messages to (queue name)
    @param type messaging pattern; Experimental way to configure message system.
    @brief Experimental mechanism for applications to create more
    sophisticated message patterns.
    @retval defer.Deferred that fires a consumer instance

    notes:
    Create a consumer based on an existing queue/ messaging pattern set up in
    the broker.

    """
    amqp_conf = config_factory(name)
    consumer = messaging.Consumer(Container.instance.message_space, **amqp_conf)
    yield consumer.declare()

