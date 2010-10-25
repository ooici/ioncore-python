#!/usr/bin/env python

"""
@file ion/services/cei/queuestat.py
@author David LaBissoniere
@brief Provides subscription services to AMQP queue statistics
"""

import os

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from twisted.internet.task import LoopingCall
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory
from txrabbitmq.service import RabbitMQControlService
import twotp.node

DEFAULT_INTERVAL_SECONDS = 3.0
DEFAULT_COOKIE_PATH = '~/.erlang.cookie'

class QueueStatService(ServiceProcess):
    """Queue stat subscription service

    Only works on a RabbitMQ server running *on localhost*
    """

    declare = ServiceProcess.service_declare(name='queuestat', 
            version='0.1.0', dependencies=[])

    def slc_init(self):
        erlang_cookie = self.spawn_args.get('erlang_cookie', None)
        if not erlang_cookie:
            cookie_path = self.spawn_args.get('erlang_cookie_path', None)
            erlang_cookie = read_cookie(cookie_path)

        rabbitmq_node = self.spawn_args.get('rabbitmq_node', 'rabbit@localhost')
        node_name = twotp.node.buildNodeName(rabbitmq_node)
        twotp_process = twotp.node.Process(node_name, erlang_cookie)
        self.rabbitmq = RabbitMQControlService(twotp_process, node_name)

        self.interval = self.spawn_args.get('interval_seconds', 
                DEFAULT_INTERVAL_SECONDS)
        self.loop = LoopingCall(self._do_poll)

        # a dict of sets of (subscriber,op) tuples
        self.watched_queues = {}

    def op_watch_queue(self, content, headers, msg):
        """Start watching a queue for updates. If queue is already being
        watched by this subscriber, this operation does nothing.
        """
        log.debug("op_watch_queue content:"+str(content))
        queue_name = content.get('queue_name')
        subscriber_name = content.get('subscriber_name')
        subscriber_op = content.get('subscriber_op')

        sub_tuple = (subscriber_name, subscriber_op)

        queue_subs = self.watched_queues.get(queue_name, None)
        if queue_subs is None:
            queue_subs = set()
            self.watched_queues[queue_name] = queue_subs
        queue_subs.add(sub_tuple)

        if not self.loop.running:
            log.debug('starting LoopingCall, to poll queues')
            self.loop.start(self.interval)

    def op_unwatch_queue(self, content, headers, msg):
        """Stop watching a queue. If queue is not being watched by subscriber,
        this operation does nothing.
        """
        log.debug("op_unwatch_queue content:"+str(content))
        queue_name = content.get('queue_name')
        subscriber_name = content.get('subscriber_name')
        subscriber_op = content.get('subscriber_op')

        sub_tuple = (subscriber_name, subscriber_op)
        
        queue_subs = self.watched_queues.get(queue_name, None)
        if queue_subs:
            queue_subs.discard(sub_tuple)
            if not queue_subs:
                del self.watched_queues[queue_name]

        if not self.watched_queues and self.loop.running:
            log.debug('No queues are being watched, disabling LoopingCall')
            self.loop.stop()
    
    @defer.inlineCallbacks
    def _do_poll(self):
        if len(self.watched_queues) == 0:
            log.debug('No queues are being watched, not querying RabbitMQ')
            defer.returnValue(None)
        log.debug('Querying RabbitMQ for queue information')

        all_queues = yield self.rabbitmq.list_queues()
        for name, info in all_queues['result']:
            subscribers = self.watched_queues.get(name, None)
            if not subscribers:
                continue
            queue_length = info['messages']
            log.debug('Length of queue %s: %s messages', name, queue_length)

            message = {'queue_name' : name, 'queue_length' : queue_length}
            yield self._notify_subscribers(list(subscribers), message)

    @defer.inlineCallbacks
    def _notify_subscribers(self, subscribers, message):
        for name, op in subscribers:
            log.debug('Notifying subscriber %s (op: %s): %s', name, op, message)
            yield self.send(name, op, message)


class QueueStatClient(ServiceClient):
    """Client for managing subscriptions to queue stat information
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "queuestat"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def watch_queue(self, queue_name, subscriber, op):
        """Watch a queue. 
        
        Updates will be sent to specified subscriber and operation
        """
        yield self._check_init()

        message = {'queue_name' : queue_name, 'subscriber_name' : subscriber,
                'subscriber_op' : op}
        log.debug("Sending QueueStat watch request: %s", message)
        yield self.send('watch_queue', message)

    @defer.inlineCallbacks
    def unwatch_queue(self, queue_name, subscriber, op):
        """Stop watching a queue. 
        """
        yield self._check_init()

        message = {'queue_name' : queue_name, 'subscriber_name' : subscriber,
                'subscriber_op' : op}
        log.debug("Sending QueueStat unwatch request: %s", message)
        yield self.send('unwatch_queue', message)

def read_cookie(path=None):
    """Reads Erlang cookie file
    """
    cookie_path = path or DEFAULT_COOKIE_PATH
    log.debug('Reading erlang cookie from ' + cookie_path)
    f = open(os.path.expanduser(cookie_path))
    try:
        return f.read().strip()
    finally:
        if f:
            f.close()

# Direct start of the service as a process with its default name
factory = ProcessFactory(QueueStatService)
