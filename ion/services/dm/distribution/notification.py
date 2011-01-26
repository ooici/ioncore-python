#!/usr/bin/env python

"""
@file ion/services/dm/distribution/notification.py
@author Dave Foster <dfoster@asascience.com>
@brief Notification classes for doing notification (logging + otherwise)
"""

from ion.core.messaging.messaging import Publisher
import logging

NOTIFY_EXCHANGE_SPACE = 'notify_exchange'
NOTIFY_EXCHANGE_TYPE = 'topic'

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
    """
    A Python Logging Handler derived class for sending log messages to AMQP.
    
    This Handler is initialized very early in the system, with the logging config,
    which presents a number of challenges.
    - The LoggingPublisher must wait until the default container is initialized
      in order to be created, as it needs an ExchangeSpace to publish to.
    - There is no good "post initialization" time to do this, so we wait until
      the first emit call comes in from Python.
    - Log messages emitted are queued until they are able to be sent via flush, which
      is called when the LoggingPublisher finished initialization.
    """

    def __init__(self, level=logging.NOTSET):
        logging.Handler.__init__(self, level)
        self._log_pub = None
        self._stored_logs = []
        self._create_called = False

    def emit(self, record):

        # lazy initialize 
        if not self._log_pub and not self._create_called:
            self._create_publisher()

        if self._log_pub:
            self._send_log_msg(record)
        else:
            self._stored_logs.append(record)

    def flush(self):

        # can't do anything if we have no publisher yet
        if not self._log_pub:
            return

        for record in self._stored_logs:
            self._send_log_msg(record)

        self._stored_logs = []

    def _create_publisher(self):
        assert self._log_pub == None
        import ion.core.ioninit

        def publisher_inited(result):
            self._log_pub = result
            self.flush()

        # begin deferred creation
        log_pub_def = LoggingPublisher.name(ion.core.ioninit.container_instance.exchange_manager.exchange_space, {})
        log_pub_def.addCallback(publisher_inited)

        self._create_called = True

    def _send_log_msg(self, rec):
        assert isinstance(self._log_pub, LoggingPublisher)
        getattr(self._log_pub, rec.levelname.lower())(rec.msg, name=rec.name, record=rec)

