#!/usr/bin/env python

"""
@file ion/test/loadtests/brokerload.py
@author Michael Meisinger
@author Adam R. Smith
@brief Creates load on an AMQP broker
"""

import uuid

from twisted.internet import defer, reactor

from carrot import connection, messaging

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.test.loadtest import LoadTest, LoadTestOptions
import ion.util.procutils as pu

import sys

class BrokerTestOptions(LoadTestOptions):
    optParameters = [
          ['scenario', 's', 'message', 'Load test scenario.']
        , ['host', 'h', 'localhost', 'Broker host name.']
        , ['port', 'p', 5672, 'Broker port.']
        , ['vhost', 'v', '/', 'Broker vhost.']
        , ['heartbeat', 'hb', 0, 'Heartbeat rate [seconds].']
        , ['monitor', 'm', 3, 'Monitor poll rate [seconds].']

        # Tuning parameters for custom test
        , ['exchange', None, None, 'Name of exchange for distributed tests.']
        , ['queue', None, None, 'Name of queue for distributed tests. Supplying a name sets the "exclusive" queue parameter to false.']
        , ['route', None, None, 'Name of routing key for distributed tests.']
        , ['consume', None, True, 'Whether to run a consumer. Set to false to try and make the broker explode.']
        , ['ack', None, True, 'Whether the consumer should "ack" messages. Set to false to try and make the broker explode.']
        , ['exchanges', None, 1, 'Number of concurrent exchanges per connection. Note that setting the "exchange" name overrides this.']
        , ['routes', None, 1, 'Number of concurrent routes per exchange. Note that setting the "route" name overrides this.']
        , ['queues', None, 1, 'Number of concurrent queues per route. Note that setting the "queue" name overrides this.']
        , ['publishers', None, 1, 'Number of concurrent publishers per route.']
        , ['consumers', None, 1, 'Number of concurrent consumers per queue.']
    ]
    optFlags = []


class BrokerTest(LoadTest):

    def setUp(self, argv=None):
        fullPath = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        if argv is None and fullPath in sys.argv:
            argv = sys.argv[sys.argv.index(fullPath) + 1:]
        self.opts = opts = BrokerTestOptions()
        opts.parseOptions(argv)

        self.scenario = opts['scenario']
        self.broker_host = opts['host']
        self.broker_port = opts['port']
        self.broker_vhost = opts['vhost']
        self.monitor_rate = opts['monitor']

        print 'Running the "%s" scenario on "%s:%d" at "%s".' % (self.scenario, self.broker_host,
                                                                 self.broker_port, self.broker_vhost)

        self.cur_state['connects'] = 0
        self.cur_state['msgsend'] = 0
        self.cur_state['msgrecv'] = 0
        self.cur_state['errors'] = 0

        self.publishers, self.consumers = [], []

        self._enable_monitor(self.monitor_rate)

    def guid(self):
        return '%s:%s' % (self.load_id, pu.create_guid())

    @defer.inlineCallbacks
    def generate_load(self):
        opts = self.opts
        
        if self.scenario == "connect":
            while True:
                if self.is_shutdown():
                    break
                yield self._connect_broker()
                yield self._disconnect_broker()

        elif self.scenario == "message":
            yield self._connect_broker()

            # Generate a bunch of unique ids for each parameter if an explicit name was not supplied
            exchange, queue, route = opts['exchange'], opts['queue'], opts['route']
            exchanges = [exchange] if exchange else [self.guid() for i in range(opts['exchanges'])]
            routes = [route] if route else [self.guid() for i in range(opts['routes'])]
            queues = [queue] if queue else [self.guid() for i in range(opts['queues'])]

            print 'Exchanges: %s, queues: %s, routes: %s' % (exchanges, queues, routes)
            
            yield self._declare_publishers(exchanges, routes)
            yield self._declare_consumers(exchanges, routes, queues)

            # Start the publisher deferred before waiting/yielding with the consumer
            self._run_publishers()
            yield self._run_consumers()


    @defer.inlineCallbacks
    def _connect_broker(self):
        self.connection = connection.BrokerConnection(hostname=self.broker_host, port=self.broker_port,
                                                      virtual_host=self.broker_vhost, heartbeat=self.opts['heartbeat'])

        yield self.connection.connect()
        self.cur_state['connects'] += 1

    @defer.inlineCallbacks
    def _disconnect_broker(self):
        yield self.connection._connection.transport.loseConnection()

    @defer.inlineCallbacks
    def _declare_publishers(self, exchanges, routes, exchange_type='topic', durable=False, auto_delete=True):
        self.publishers = []
        defers = []
        backend = self.connection.create_backend()

        for exchange in exchanges:
            defers.append(backend.exchange_declare(exchange=exchange, type=exchange_type,
                                                       durable=durable, auto_delete=auto_delete))

            for route in routes:
                pub = messaging.Publisher(connection=self.connection, exchange_type=exchange_type, durable=durable,
                    auto_delete=auto_delete, exchange=exchange, routing_key=route)
                self.publishers.append(pub)

        yield defer.DeferredList(defers)

    @defer.inlineCallbacks
    def _declare_consumers(self, exchanges, routes, queues, exchange_type='topic', durable=False, auto_delete=True,
                           exclusive=False, no_ack=True):
        self.consumers = []
        qdecs, qbinds = [], []
        for exchange in exchanges:
            for route in routes:
                for queue in queues:
                    con = messaging.Consumer(connection=self.connection, exchange=exchange, exchange_type=exchange_type,
                        durable=durable, auto_delete=auto_delete, exclusive=exclusive, no_ack=no_ack, routing_key=route)

                    self.consumers.append(con)
                    con.register_callback(self._recv_callback)

                    qdecs.append(con.backend.queue_declare(queue=queue, durable=durable, exclusive=exclusive,
                                                           auto_delete=auto_delete, warn_if_exists=con.warn_if_exists))
                    qbinds.append(con.backend.queue_bind(queue=queue, exchange=exchange,
                                                         routing_key=route, arguments={}))

                    #yield self.consumer.qos()

        yield defer.DeferredList(qdecs)
        yield defer.DeferredList(qbinds)


    @defer.inlineCallbacks
    def _run_consumers(self):
        yield defer.DeferredList([con.iterconsume() for con in self.consumers])

        while True:
            if self.is_shutdown():
                break

            # Check on the consumers less frequently, up to 50times/sec
            yield pu.asleep(0.02)

    @defer.inlineCallbacks
    def _run_publishers(self):
        while True:
            if self.is_shutdown():
                break
            yield self._send_messages()
            # Generate as much load as this client can handle, up to 100000/sec
            yield pu.asleep(0.00001)


    @defer.inlineCallbacks
    def _send_messages(self):
        message = """Lorem ipsum dolor sit amet, consectetur adipisicing elit,
        sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut
        enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut
        aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit
        in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
        Excepteur sint occaecat cupidatat non proident, sunt in culpa qui
        officia deserunt mollit anim id est laborum."""

        sends = [pub.send(message, content_type='binary', content_encoding='binary') for pub in self.publishers]
        yield defer.DeferredList(sends)

        self.cur_state['msgsend'] += len(sends)

    def _recv_callback(self, message):
        self.cur_state['msgrecv'] += 1
        message.ack()

    @defer.inlineCallbacks
    def tearDown(self):
        yield defer.DeferredList([pub.close() for pub in self.publishers])
        yield defer.DeferredList([con.close() for con in self.consumers])
        yield self._disconnect_broker()

        self._disable_monitor()
        self._call_monitor()

    def monitor(self):
        interval = self._get_interval()

        rates = self._get_state_rate()
        print "%s: new state  %s" % (self.load_id, self.cur_state)
        print "%s: rate state %s" % (self.load_id, rates)

        #print "%s: performed %s connect (rate %s), %s send, %s receive, %s error" % (
        #    self.load_id, self.connects, connect_rate, self.msgsend, self.msgrecv, self.errors)


"""
python -m ion.test.load_runner -s -c ion.test.loadtests.brokerload.BrokerTest -s connect
"""
