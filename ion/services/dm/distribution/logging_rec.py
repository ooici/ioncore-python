#!/usr/bin/env python

"""
@file ion/services/dm/distribution/logging_rec.py
@author Dave Foster <dfoster@asascience.com>
@brief Receiver for logging messages over AMQP

This must be defined in its own file due to Receiver's dependencies on ioninit.
"""

from ion.core.messaging.receiver import Receiver
from twisted.internet import defer

from ion.services.dm.distribution.notification import NOTIFY_EXCHANGE_SPACE, NOTIFY_EXCHANGE_TYPE

class LoggingPublisherReceiver(Receiver):

    def __init__(self, name, **kwargs):
        """
        Constructor override.
        Sets up publisher config for using our notification exchange, used by send.
        """
        kwargs = kwargs.copy()

        # add our custom exchange stuff to be passed through to the ultimate result of send
        kwargs['publisher_config'] = { 'exchange' : NOTIFY_EXCHANGE_SPACE,
                                       'exchange_type' : NOTIFY_EXCHANGE_TYPE,
                                       'durable': False,
                                       'mandatory': True,
                                       'immediate': False,
                                       'warn_if_exists': False }

        Receiver.__init__(self, name, **kwargs)

    def debug(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(headers={}, content=msg, recipient="_not.log.DEBUG.%s" % name)

    def info(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(headers={}, content=msg, recipient="_not.log.INFO.%s" % name)

    def warn(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(headers={}, content=msg, recipient="_not.log.WARN.%s" % name)

    def error(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(headers={}, content=msg, recipient="_not.log.ERROR.%s" % name)

    def critical(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(headers={}, content=msg, recipient="_not.log.CRITICAL.%s" % name)

    def exception(self, msg, *args, **kwargs):
        name = kwargs.pop("name", "<unknown>")
        self.send(headers={}, content=msg, recipient="_not.log.EXCEPTION.%s" % name)

    warning = warn

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


