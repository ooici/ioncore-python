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
import time

class BrokerTestOptions(LoadTestOptions):
    optParameters = [
          ['scenario', 's', 'message', 'Load test scenario.']
        , ['host', 'h', 'localhost', 'Broker host name.']
        , ['port', 'p', 5672, 'Broker port.']
        , ['vhost', 'v', '/', 'Broker vhost.']
        , ['heartbeat', None, 0, 'Heartbeat rate [seconds].']
        , ['monitor', 'm', 3, 'Monitor poll rate [seconds].']

        # Tuning parameters for custom test
        , ['exchange', None, None, 'Name of exchange for distributed tests.']
        , ['queue', None, None, 'Name of queue for distributed tests. Supplying a name sets the "exclusive" queue parameter to false.']
        , ['route', None, None, 'Name of routing key for distributed tests.']

        , ['exchanges', None, 1, 'Number of concurrent exchanges per connection. Note that setting the "exchange" name overrides this.']
        , ['routes', None, 1, 'Number of concurrent routes per exchange. Note that setting the "route" name overrides this.']
        , ['queues', None, 1, 'Number of concurrent queues per route. Note that setting the "queue" name overrides this.']
        , ['publishers', None, 1, 'Number of concurrent publishers per route.']
        , ['consumers', None, 1, 'Number of concurrent consumers per queue.']
    ]
    optFlags = [
          ['no-consume', None, 'Disable message consumers to try and make the broker explode.']
        , ['no-ack', None, 'Disable message acks to try and make the broker explode.']
        , ['no-autodelete', None, 'Disable auto-deletion of exchanges and queues to try and make the broker explode.']
        , ['durable', None, 'Set durable to true in all relevant AMQP options.']
        , ['exclusive', None, 'Set queues and consumers to exclusive.']
    ]


class BrokerTest(LoadTest):
    """
    Pure AMQP load test with a large number of configuration parameters. Run it like this to see the params:
    python -m ion.test.load_runner -s -c ion.test.loadtests.brokerload.BrokerTest - --help
    """

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
        self.ack_msgs = not opts['no-ack']
        self.consume_msgs = not opts['no-consume']
        self.publish_msgs = True

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
            print '#%s] <%s> on %s:%d at %s.' % (self.load_id, self.scenario, self.broker_host,
                                                                 self.broker_port, self.broker_vhost)
            while True:
                if self.is_shutdown():
                    break
                yield self._connect_broker()
                yield self._disconnect_broker()

        elif self.scenario == "message":
            yield self._connect_broker()

            # Generate a bunch of unique ids for each parameter if an explicit name was not supplied
            exchange, queue, route = opts['exchange'], opts['queue'], opts['route']
            exchangecount, queuecount, routecount = int(opts['exchanges']), int(opts['queues']), int(opts['routes'])
            exchanges = [exchange] if exchange else [self.guid() for i in range(exchangecount)]
            routes = [route] if route else [self.guid() for i in range(routecount)]
            queues = [queue] if queue else [self.guid() for i in range(queuecount)]

            pubcount, concount = int(opts['publishers']), int(opts['consumers'])
            print '#%s] Spawning %d exchanges, %d routes, %d queues, %d total publishers, and %d total consumers.' % (
                self.load_id, len(exchanges), len(routes), len(queues), len(exchanges)*len(routes)*pubcount,
                len(exchanges)*len(routes)*len(queues)*concount
            )
            print '-'*80

            durable, exclusive, auto_delete = opts['durable'], opts['exclusive'], not opts['no-autodelete']
            if self.publish_msgs:
                yield self._declare_publishers(exchanges, routes, pubcount, durable=durable, auto_delete=auto_delete)
            if self.consume_msgs:
                yield self._declare_consumers(exchanges, routes, queues, concount, durable=durable,
                                              auto_delete=auto_delete, exclusive=exclusive)

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
    def _declare_publishers(self, exchanges, routes, count, exchange_type='topic', durable=False, auto_delete=True):
        self.publishers = []
        defers = []
        backend = self.connection.create_backend()

        for exchange in exchanges:
            defers.append(backend.exchange_declare(exchange=exchange, type=exchange_type,
                                                       durable=durable, auto_delete=auto_delete))

            for route in routes:
                for i in range(count):
                    pub = messaging.Publisher(connection=self.connection, exchange_type=exchange_type, durable=durable,
                        auto_delete=auto_delete, exchange=exchange, routing_key=route)
                    self.publishers.append(pub)

        yield defer.DeferredList(defers)

    @defer.inlineCallbacks
    def _declare_consumers(self, exchanges, routes, queues, count, exchange_type='topic', durable=False,
                           auto_delete=True, exclusive=False, no_ack=True, warn_if_exists=False):
        self.consumers = []
        qdecs, qbinds = [], []
        backend = self.connection.create_backend()

        for exchange in exchanges:
            for route in routes:
                for queue in queues:
                    qdecs.append(backend.queue_declare(queue=queue, durable=durable, exclusive=exclusive,
                            auto_delete=auto_delete, warn_if_exists=warn_if_exists))
                    qbinds.append(backend.queue_bind(queue=queue, exchange=exchange, routing_key=route, arguments={}))

                    for i in range(count):
                        con = messaging.Consumer(connection=self.connection, queue=queue, exchange=exchange,
                             exchange_type=exchange_type, durable=durable, auto_delete=auto_delete, exclusive=exclusive,
                             no_ack=no_ack, routing_key=route)

                        self.consumers.append(con)
                        con.register_callback(self._recv_callback)

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
        if self.ack_msgs: message.ack()

    @defer.inlineCallbacks
    def tearDown(self):
        yield defer.DeferredList([pub.close() for pub in self.publishers])
        yield defer.DeferredList([con.close() for con in self.consumers])
        yield self._disconnect_broker()

        self._disable_monitor()
        self._call_monitor(False)
        self.summary()


    def monitor(self, output=True):
        interval = self._get_interval()
        rates = self._get_state_rate()

        if output:
            pieces = []
            if rates['errors']: pieces.append('had %.2f errors/sec' % rates['errors'])
            if rates['connects']: pieces.append('made %.2f connects/sec' % rates['connects'])
            if rates['msgsend']: pieces.append('sent %.2f msgs/sec' % rates['msgsend'])
            if rates['msgrecv']: pieces.append('received %.2f msgs/sec' % rates['msgrecv'])
            print '#%s] (%s) %s' % (self.load_id, time.strftime('%H:%M:%S'), ', '.join(pieces))

    def summary(self):
        state = self.cur_state
        secsElapsed = (state['_time'] - self.start_state['_time']) or 0.0001

        print '\n'.join([
              '-'*80
            , '#%s Summary' % (self.load_id)
            , 'Test ran for %.2f seconds, with a total of %d sent messages and %d received messages.' % (
                  secsElapsed, state['msgsend'], state['msgrecv'])
            , 'The average messages/second was %.2f sent and %.2f received.' % (
                state['msgsend']/secsElapsed, state['msgrecv']/secsElapsed)
            , '-'*80
        ])


"""
python -m ion.test.load_runner -s -c ion.test.loadtests.brokerload.BrokerTest - -s connect
"""
