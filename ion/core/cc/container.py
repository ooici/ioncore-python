"""
@author Dorian Raymer
@author Michael Meisinger
@brief Capability Container main class

A container utilizes the messaging abstractions.
A container creates/enters a MessageSpace (a particular vhost in a
broker)(maybe MessageSpace is a bad name; maybe not, depends on what
context wants to be managed. A Message space could just be a particular
exchange. Using the vhost as the main context setter for a message space,
assuming a common broker/cluster, means a well known common exchange can be
used as a default namespace, where the names are spawnable instance ids, as
well as, permanent abstract endpoints. The same Message Space holding this
common exchange could also hold any other exchanges where the names
(routing keys) can have possibly different semantic meaning or other usage
strategies.
"""

import os
import sys
import traceback

from twisted.internet import defer
from twisted.python import log

from carrot import connection

from ion.core.messaging import messaging
from ion.core.cc.store import Store

DEFAULT_EXCHANGE_SPACE = 'magnet.topic'

"""
The integration message object has a Base definition in the carrot/backends
base.py, and a specific txamqp extension in the txamqplib backend.

@note Since the message operators live here, maybe the Integration message class
should live here too?
"""

class Id(object):
    """entity instance id
    local id is the int from Spawnable id count
    full id is container id + local id
    XXX local might be pid?
        full might be named cid (container id)?
    """

    def __init__(self, local, container=None):
        self.local = str(local)
        if container is None:
            container = Container.id
        self.container = container
        self.full = container + '.' + str(local)

    def __str__(self):
        """XXX print just local...or full?
        """
        return "%s" % str(self.full)

    def __repr__(self):
        return """Id(%s, container="%s")""" % (str(self.local),
                                             str(self.container),)

    def __eq__(self, other):
        if self.local == other.local:
            return True
        return False

    def __hash__(self):
        """this is so Id's make sense as dict keys for Spawnable.
        """
        return int(self.local)

class Name(object):
    """
    High-level messaging name.
    Encapsulates messaging (amqp) details

    Might also retain name config dict
    OR might just be the config
    """


class Container(object):
    """represents capability container.
    (like application service...)
    (like otp node..?)

    As a context, Container interfaces the messaging space with the local
    Spawnable and their Receivers...
    """

    common = 'container' # common should be exchange
    space = None # main message space
    exchange_space = None # exchange space (namespace)
    interceptor_system = None #
    id = '%s.%d' % (os.uname()[1], os.getpid(),)
    args = None  # Startup arguments
    store = Store()
    _started = False

    @staticmethod
    def connectMainSpace(broker):
        Container._started = True
        Container.space = broker
        Container.exchange_space = messaging.Exchange(Container.space,
                                                    'magnet.topic')
        return broker.connect()

    @staticmethod
    def configure_messaging(name, config):
        """
        XXX This is not acceptable: config is defined by lcaarch!!!!
        """
        if config['name_type'] == 'worker':
            name_type_f = messaging.worker
        elif config['name_type'] == 'direct':
            name_type_f = messaging.direct
        elif config['name_type'] == 'fanout':
            name_type_f = messaging.fanout
        else:
            raise RuntimeError("Invalid name_type: "+config['name_type'])

        amqp_config = name_type_f(name)
        amqp_config.update(config)
        def _cb(res):
            return messaging.Configure.name(Container.exchange_space,
                                                            amqp_config)
        d = Container.store.put(name, amqp_config)
        d.addCallback(_cb)
        return d


@defer.inlineCallbacks
def new_consumer(name_config, target):
    """given spawnable instance Id, create consumer
    using hardcoded name conventions

    @param id should be of type Id
    @retval defer.Deferred that fires a consumer instance
    """
    consumer = yield messaging.BaseConsumer.name(Container.exchange_space, name_config)
    if Container.interceptor_system:
        wrapped_target = Container.interceptor_system.in_stack(target)
    else:
        wrapped_target = target
    consumer.register_callback(wrapped_target.send)
    consumer.iterconsume()
    defer.returnValue(consumer)

@defer.inlineCallbacks
def new_publisher(name_config):
    """
    """
    publisher = yield messaging.Publisher.name(Container.exchange_space, name_config)
    if Container.interceptor_system:
        @coroutine
        def _genfunc(func):
            """Generator function that calls the argument"""
            msg = (yield)
            func(msg)
        publisher_send_gen = _genfunc(publisher.send)
        publisher.send = Container.interceptor_system.out_stack(publisher_send_gen)
    defer.returnValue(publisher)


class MessageSpace(connection.BrokerConnection):

    def __repr__(self):
        params = ['hostname',
                'userid',
                'password',
                'virtual_host',
                ]
        s = "MessageSpace("
        for param in params:
            s += param +"='%s', " % getattr(self, param)
        s += "port=%d" % self.port
        s += ")"
        return s



def coroutine(func):
    """decorator for coroutine functions
    """
    def start(*args, **kwargs):
        g = func(*args, **kwargs)
        g.next()
        return g
    return start

@coroutine
def filter_fixture(transform, target):
    """
    @brief General scaffold for a pipeline of message interceptor/filters.
    @param transform A function that accepts a Message object as its
    argument.
    @param target Next destination to send Message object.
    """
    while True:
        msg = (yield)
        try:
            if type(transform) is list:
                msg_prime = transform[0](msg)
            else:
                msg_prime = transform(msg)
            if msg_prime:
                target.send(msg_prime)
        except StandardError, e:
            print 'Exception in interceptor: '+repr(e)
            (etype, value, trace) = sys.exc_info()
            traceback.print_tb(trace)

def pass_transform(msg):
    """Trivial identity transform -- pass all
    """
    log.msg('pass_transform', msg)
    return msg

def drop_transform(msg):
    """Drop all messages
    """
    log.msg('drop_transform', msg)
    return None


class Interceptor(object):
    """template/interface of a message interceptor
    """

    def __init__(self, target):
        self.target = target

    def send(self, msg):
        """implement functionality
        default sends to next target.
        """
        self.target.send(msg)

class InterceptorSystem(object):

    def __init__(self):
        self._id_implementation = [pass_transform]
        self._policy_implementation = [pass_transform]
        self._governance_implementation = [pass_transform]

    def registerIdmInterceptor(self, transform):
        """install function that implements validation/decoration
        of a message's identity.
        The function is a decision point:
            for incoming messages, the decision to pass the message on is
                based on success of validating the message identity.
            for outgoing messages, the message is officially signed with
                the identity of (this container?)
        """
        self._id_implementation[0] = transform

    def registerPolicyInterceptor(self, transform):
        """install function that enforces basic policy in the context of
        the message identity and the integration level message headers.
        The function is a decision point:
            messages that comply with policy are passed through.
        """
        self._policy_implementation[0] = transform

    def registerGovernanceInterceptor(self, transform):
        """install function that decides whether or not to pass a message
        based on some examination of the entire message.
        """
        self._governance_implementation[0] = transform

    def in_stack(self, target):
        """incoming message interception stack, per consumer.
        the target should be the callback function given to the messaging
        consumer
        (prelim)
        """
        return self.id_check(self.policy_check(self.governance_check(target)))

    def out_stack(self, target):
        """outgoing message interception stack, per consumer.
        the target should be the callback function given to the messaging
        consumer
        (prelim)
        """
        def _sendf(gent):
            """Returns a function that calls a generator"""
            def _send(msg):
                """Calls a generator with msg arg"""
                try: gent.send(msg)
                except StopIteration: pass
            return _send
        return _sendf(self.governance_check(self.policy_check(self.id_check(target))))

    def id_check(self, target):
        return filter_fixture(self._id_implementation, target)

    def policy_check(self, target):
        return filter_fixture(self._policy_implementation, target)

    def governance_check(self, target):
        return filter_fixture(self._governance_implementation, target)



def startContainer(config):
    """
    This could eventually be more like a factory, but so far, there is only
    supposed to be on Container (per python process), so the Container is
    mostly used as a class with out instantiating (facilitating a kind of
    singleton pattern)
    """
    if Container._started:
        raise RuntimeError('Already started')
    hostname = config['broker_host']
    port = config['broker_port']
    virtual_host = config['broker_vhost']
    heartbeat = int(config['broker_heartbeat'])
    Container.args = config.get('args', None)
    Container.interceptor_system = InterceptorSystem() # hack; time for inst of Container
    broker = messaging.MessageSpace(hostname=hostname,
                            port=port,
                            virtual_host=virtual_host,
                            heartbeat=heartbeat)
    return Container.connectMainSpace(broker)

def test():
    broker = MessageSpace()
    return Container.connectMainSpace(broker)
