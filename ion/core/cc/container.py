#!/usr/bin/env python

"""
@author Dorian Raymer
@author Michael Meisinger
@brief Capability Container main class
@see http://www.oceanobservatories.org/spaces/display/syseng/CIAD+COI+SV+Python+Capability+Container

A container utilizes the messaging abstractions for AMQP.

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

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.cc.store import Store
from ion.core.id import Id
from ion.core.messaging import messaging
from ion.core.messaging.messaging import MessageSpace, Publisher, Consumer
from ion.core.messaging.messaging import ProcessExchangeSpace
from ion.core.pack.application import AppLoader
from ion.util.state_object import BasicLifecycleObject

DEFAULT_EXCHANGE_SPACE = 'magnet.topic'

class Container(BasicLifecycleObject):
    """
    Represents an instance of the Capability Container. Typically, in one Twisted
    process (= one UNIX process), there is only one instance of a CC. In test cases,
    however, there might be more.

    As a context, Container interfaces the messaging space with the local
    Spawnable and their Receivers...
    """

    # Static: the one instance of a Container
    instance = None

    # Static variables
    id = '%s.%d' % (os.uname()[1], os.getpid())
    args = None  # Startup arguments
    store = Store()
    _started = False

    def __init__(self):
        BasicLifecycleObject.__init__(self)

        # Config instance
        self.config = None

        # Container broker connection / vhost parameters
        self.message_space = None

        # Broker connection instance
        self.broker_connection = None

        # Static: Default exchange space
        self.exchange_space = None

    def on_initialize(self, config, *args, **kwargs):
        """
        Initializes the instance of a container
        """
        self.config = config

        # Set additional container args
        Container.args = self.config.get('args', None)

        # Configure the broker connection
        hostname = self.config['broker_host']
        port = self.config['broker_port']
        virtual_host = self.config['broker_vhost']
        heartbeat = int(self.config['broker_heartbeat'])

        # Is a BrokerConnection instance (no action at this point)
        self.message_space = MessageSpace(hostname=hostname,
                                port=port,
                                virtual_host=virtual_host,
                                heartbeat=heartbeat)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        Activates the container
        @retval Deferred
        """
        Container._started = True

        self.exchange_space = ProcessExchangeSpace(message_space=self.message_space,
                                                   name=DEFAULT_EXCHANGE_SPACE)
        yield self.message_space.activate()

    def on_deactivate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        yield self.message_space.terminate()
        #log.info("Container closed")
        Container._started = False
        self.store = Store()

    def on_error(self, *args, **kwargs):
        raise RuntimeError("Illegal state change for container")

    def start_app(self, app_filename):
        """
        Start a Capability Container application from an .app file.
        @see OTP design principles, applications
        """
        log.info("Starting app: %s" % app_filename)
        app = AppLoader.load_app_definition(app_filename)

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
            return Consumer.name(Container.instance.exchange_space, amqp_config)
        d = Container.store.put(name, amqp_config)
        d.addCallback(_cb)
        return d

    @defer.inlineCallbacks
    def new_consumer(self, name_config, target):
        """
        Given spawnable instance Id, create consumer
        using hardcoded name conventions

        @param id should be of type Id
        @retval defer.Deferred that fires a consumer instance
        """
        consumer = yield Consumer.name(self.exchange_space, name_config)
        consumer.register_callback(target.send)
        consumer.iterconsume()
        defer.returnValue(consumer)

    def send(self, to_name, message_data, exchange_space=None):
        """
        Sends a message
        """
        exchange_space = exchange_space or self.exchange_space
        return exchange_space.send(to_name, message_data)

def create_new_container():
    """
    Factory for a container.
    This also makes sure that only one container is active at any time,
    currently.
    """
    if Container._started:
        raise RuntimeError('Already started')

    c = Container()
    Container.instance = c

    return c

Id.default_container_id = Container.id
