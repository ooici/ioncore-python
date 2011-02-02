#!/usr/bin/env python

"""
@file ion/services/dm/distribution/notification.py
@author Dave Foster <dfoster@asascience.com>
@brief Notification classes for doing notification (logging + otherwise)
"""

import logging

NOTIFY_EXCHANGE_SPACE = 'notify_exchange'
NOTIFY_EXCHANGE_TYPE = 'topic'

class LoggingHandler(logging.Handler):
    """
    A Python Logging Handler derived class for sending log messages to AMQP.
    
    This Handler is initialized very early in the system, with the logging config,
    which presents a number of challenges.
    - The LoggingPublisherReceiver must wait until the default container is initialized
      in order to be imported or created.
    - There is no good "post initialization" time to do this, so we wait until
      the first emit call comes in from Python.
    - Log messages emitted are queued until they are able to be sent via flush, which
      is called when the LoggingPublisherReceiver/its attached anon process finishes
      initialization.
    """

    def __init__(self, level=logging.NOTSET):
        logging.Handler.__init__(self, level)
        self._log_pub = None
        self._stored_logs = []
        self._create_called = False

        self._all = []

    def emit(self, record):

        print "handler", dir(self)
        print "record", dir(record)


        # lazy initialize 
        if not self._log_pub and not self._create_called:
            self._create_publisher()

        self._all.append(record)

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
        from ion.services.dm.distribution.logging_rec import LoggingPublisherReceiver
        from ion.core.process.process import Process

        def publisher_inited(result, pub=None):
            self._log_pub = pub
            self.flush()

        # begin deferred creation (of Process)
        proc = Process()
        log_pub = LoggingPublisherReceiver("loghandler", process=proc)
        spawn_def = proc.spawn()
        spawn_def.addCallback(publisher_inited, pub=log_pub)

        self._create_called = True

    def _send_log_msg(self, rec):
        self._log_pub.log(rec)

