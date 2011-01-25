#!/usr/bin/env python

"""
@file ion/services/dm/distribution/notification.py
@author Dave Foster <dfoster@asascience.com>
@brief Notification classes for doing notification (logging + otherwise)
"""

from ion.core.messaging.messaging import Publisher
from ion.core.messaging.receiver import Receiver
from twisted.internet import defer
import logging

NOTIFY_EXCHANGE_SPACE = 'notify_exchange'
NOTIFY_EXCHANGE_TYPE = 'topic'

class LoggingReceiver(Receiver):

    def __init__(self, *args, **kwargs):
        kwargs = kwargs.copy()
        kwargs['handler'] = self.printlog
        self.loglevel = kwargs.pop('loglevel', 'WARN')
        Receiver.__init__(self, *args, **kwargs)

        self._msgs = []

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"
        yield self._init_receiver({}, store_config=True)

    @defer.inlineCallbacks
    def _init_receiver(self, receiver_config, store_config=False):
        receiver_config = receiver_config.copy()
        receiver_config.update( {'exchange': NOTIFY_EXCHANGE_SPACE,
                                 'exchange_type':  NOTIFY_EXCHANGE_TYPE,
                                 'durable': False,
                                 'queue': None,     # make it up for us
                                 'binding_key': "_not.log.idontexist.#",
                                 'routing_key': "_not.log.idontexist.#",
                                 'exclusive': False,
                                 'mandatory': True,
                                 'warn_if_exists' : False,
                                 'no_ack': False,
                                 'auto_delete' : True,
                                 'immediate' : False })

        yield Receiver._init_receiver(self, receiver_config, store_config)

        # add logging bindings to self.consumer
        levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL', 'EXCEPTION']
        try:
            idx = levels.index(self.loglevel)
        except ValueError:
            idx = levels[2]

        for level in levels[idx:]:
            self.consumer.backend.queue_bind(queue=self.consumer.queue,
                                             exchange=self.consumer.exchange,
                                             routing_key="_not.log.%s.#" % level,
                                             arguments={})

    def printlog(self, payload, msg):
        self._msgs.append(msg)
        print "LOG", payload
        msg.ack()

class LoggingPublisher(Publisher):
    @classmethod
    def name(cls, ex_space, config):

        connection = ex_space.connection
        if config == None:
            raise RuntimeError("LoggingPublisher.name(): no config given")

        config = config.copy()
        config.update( { 'exchange' : NOTIFY_EXCHANGE_SPACE,
                         'exchange_type' : NOTIFY_EXCHANGE_TYPE,
                       })

        full_config = ex_space.exchange.config_dict.copy()
        full_config.update(config)

        inst = cls(connection, **full_config)

        return inst.declare()

    def debug(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(msg, routing_key="_not.log.DEBUG.%s" % name)

    def info(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(msg, routing_key="_not.log.INFO.%s" % name)

    def warn(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(msg, routing_key="_not.log.WARN.%s" % name)

    def error(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(msg, routing_key="_not.log.ERROR.%s" % name)

    def critical(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(msg, routing_key="_not.log.CRITICAL.%s" % name)

    def exception(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(msg, routing_key="_not.log.EXCEPTION.%s" % name)

    warning = warn

class LoggingHandler(logging.Handler):
    def __init__(self, ex_space, config, level=logging.NOTSET):
        logging.Handler.__init__(self, level)
        self._log_pub = LoggingPublisher.name(ex_space, config)

    def emit(self, record):

        # ugh hack
        if isinstance(self._log_pub, defer.Deferred):
            if not self._log_pub.result:
                raise StandardError("Publisher never initialized")

            self._log_pub = self._log_pub.result

        getattr(self._log_pub, record.levelname.lower())(record.msg, name=record.name, record=record)



