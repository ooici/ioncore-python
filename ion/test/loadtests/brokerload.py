#!/usr/bin/env python

"""
@file ion/test/loadtests/brokerload.py
@author Michael Meisinger
@brief Creates load on an AMQP broker
"""

import uuid

from twisted.internet import defer

from carrot import connection, messaging

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.test.loadtest import LoadTest, LoadTestOptions
import ion.util.procutils as pu

class BrokerTestOptions(LoadTestOptions):
    optParameters = [
                ["scenario", "s", "connect", "Load test scenario"],
                ["host", "h", "localhost", "Broker host name"],
                ["port", "p", 5672, "Broker port"],
                ["vhost", "v", "/", "Broker vhost"],
                ["heartbeat", None, 0, "Heartbeat rate [seconds]"],
                    ]
    optFlags = [
                ]


class BrokerTest(LoadTest):

    def setUp(self):
        numopt = len(self.options['test_args'])
        assert numopt >= 2
        self.scenario = self.options['test_args'][0]
        self.broker_host = self.options['test_args'][1]
        self.broker_port = numopt >= 3 and self.options['test_args'][2] or 5672
        self.broker_vhost = numopt >= 4 and self.options['test_args'][3] or "/"

        self.cur_state['connects'] = 0
        self.cur_state['msgsend'] = 0
        self.cur_state['msgrecv'] = 0
        self.cur_state['errors'] = 0

        self._enable_monitor(5)

    @defer.inlineCallbacks
    def generate_load(self):
        if self.scenario == "connect":
            while True:
                if self.is_shutdown():
                    break
                yield self._connect_broker()
                yield self._disconnect_broker()

        elif self.scenario == "send":
            yield self._connect_broker()
            exname = "%s:%s" % (self.load_id,pu.create_guid())
            queuename = "%s:%s" % (self.load_id,pu.create_guid())
            routingkey = "%s:%s" % (self.load_id,pu.create_guid())
            yield self._declare_exchange(exname, routingkey)
            yield self._declare_consumer(exname, routingkey, queuename)
            while True:
                if self.is_shutdown():
                    break
                yield self._send_message()
                yield pu.asleep(0.02)

            yield self.publisher.close()
            yield self.consumer.close()
            yield self._disconnect_broker()
            #print "disconnected"

        elif self.scenario == "wait":
            while True:
                if self.is_shutdown():
                    break
                yield pu.asleep(1)

        elif self.scenario == "short":
            for i in range(5):
                if self.is_shutdown():
                    break
                yield pu.asleep(1)


    @defer.inlineCallbacks
    def _connect_broker(self):
        self.connection = connection.BrokerConnection(
                    hostname=self.broker_host,
                    port=self.broker_port,
                    virtual_host=self.broker_vhost,
                    heartbeat=0)

        yield self.connection.connect()
        self.cur_state['connects'] += 1

    @defer.inlineCallbacks
    def _disconnect_broker(self):
        yield self.connection._connection.transport.loseConnection()

    @defer.inlineCallbacks
    def _declare_exchange(self, exname, routingkey):
        self.publisher = messaging.Publisher(
                    connection=self.connection,
                    exchange=exname,
                    exchange_type="topic",
                    durable=False,
                    auto_delete=True,
                    routing_key=routingkey)

        yield self.publisher.backend.exchange_declare(
                    exchange=self.publisher.exchange,
                    type=self.publisher.exchange_type,
                    durable=self.publisher.durable,
                    auto_delete=self.publisher.auto_delete)

    @defer.inlineCallbacks
    def _declare_consumer(self, exname, routingkey, queuename):
        self.consumer = messaging.Consumer(
                    connection=self.connection,
                    exchange=exname,
                    exchange_type="topic",
                    durable=False,
                    auto_delete=True,
                    exclusive=False,
                    routing_key=routingkey)

        yield self.consumer.backend.queue_declare(
                    queue=queuename,
                    durable=self.consumer.durable,
                    exclusive=self.consumer.exclusive,
                    auto_delete=self.consumer.auto_delete,
                    warn_if_exists=self.consumer.warn_if_exists)

        yield self.consumer.backend.queue_bind(
                    queue=queuename,
                    exchange=exname,
                    routing_key=routingkey,
                    arguments={})

        yield self.consumer.qos(prefetch_count=1)

    @defer.inlineCallbacks
    def _send_message(self):
        message = """Lorem ipsum dolor sit amet, consectetur adipisicing elit,
        sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut
        enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut
        aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit
        in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
        Excepteur sint occaecat cupidatat non proident, sunt in culpa qui
        officia deserunt mollit anim id est laborum."""

        yield self.publisher.send(
                    message,
                    content_type='binary',
                    content_encoding='binary')
        self.cur_state['msgsend'] += 1

    def tearDown(self):
        self.monitor()

    def monitor(self):
        interval = self._get_interval()

        rates = self._get_state_rate()
        print "%s: new state  %s" % (self.load_id, self.cur_state)
        print "%s: rate state %s" % (self.load_id, rates)

        #print "%s: performed %s connect (rate %s), %s send, %s receive, %s error" % (
        #    self.load_id, self.connects, connect_rate, self.msgsend, self.msgrecv, self.errors)


"""
python -m ion.test.load_runner -s -c ion.test.loadtests.brokerload.BrokerTest -p connect
"""
