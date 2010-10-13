#!/usr/bin/env python

"""
@author Dorian Raymer
@author Michael Meisinger
@brief ION Exchange manager for CC.
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.messaging import messaging
from ion.core.messaging.messaging import MessageSpace, ProcessExchangeSpace, Consumer
from ion.util.state_object import BasicLifecycleObject

DEFAULT_EXCHANGE_SPACE = 'magnet.topic'

class ExchangeManager(BasicLifecycleObject):
    """
    Manager class for capability container exchange management.
    """

    def __init__(self, container):
        BasicLifecycleObject.__init__(self)
        self.container = container

        # Container broker connection / vhost parameters
        self.message_space = None

        # Default exchange space
        self.exchange_space = None

    # Life cycle

    def on_initialize(self, config, *args, **kwargs):
        """
        """
        self.config = config

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

        return defer.succeed(None)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        # Initiate the broker connection
        yield self.message_space.activate()
        self.exchange_space = ProcessExchangeSpace(
                message_space=self.message_space,
                name=DEFAULT_EXCHANGE_SPACE)

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """

        # Close the broker connection
        yield self.message_space.terminate()

    def on_error(self, *args, **kwargs):
        raise RuntimeError("Illegal state change for ExchangeManager")

    # API

    @defer.inlineCallbacks
    def declare_messaging(self, messagingCfg, cgroup=None):
        """
        Configures messaging resources.
        @todo this needs to be called from exchange management service
        """
        # for each messaging resource call Magnet to define a resource
        for name, msgResource in messagingCfg.iteritems():
            scope = msgResource.get('args',{}).get('scope','global')
            msgName = name
            if scope == 'local':
                msgName = self.container.id + "." + msgName
            elif scope == 'system':
                # @todo: in the root bootstrap this is ok, but HACK
                msgName = self.container.id + "." + msgName

            # declare queues, bindings as needed
            log.info("Messaging name config: name="+msgName+', '+str(msgResource))
            yield self.configure_messaging(msgName, msgResource)

    @defer.inlineCallbacks
    def configure_messaging(self, name, config):
        """
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
        res = yield Consumer.name(self.exchange_space, amqp_config)
        yield self.exchange_space.store.put(name, amqp_config)
        defer.returnValue(res)

    @defer.inlineCallbacks
    def new_consumer(self, name_config):
        """
        @brief create consumer
        @retval Deferred that fires a consumer instance
        """
        consumer = yield Consumer.name(self.exchange_space, name_config)
        defer.returnValue(consumer)

    def send(self, to_name, message_data, exchange_space=None):
        """
        Sends a message
        """
        exchange_space = exchange_space or self.container.exchange_manager.exchange_space
        return exchange_space.send(to_name, message_data)
