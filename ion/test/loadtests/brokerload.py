#!/usr/bin/env python

"""
@file ion/test/loadtests/brokerload.py
@author Michael Meisinger
@brief Creates load on an AMQP broker
"""

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
                #print "%s: connect to %s:%s" % (self.load_id, self.broker_host, self.broker_port)

                self.connection = connection.BrokerConnection(
                                        hostname=self.broker_host,
                                        port=self.broker_port,
                                        virtual_host=self.broker_vhost,
                                        heartbeat=0)

                yield self.connection.connect()
                #print "%s: connected" % (self.load_id)

                #yield pu.asleep(0.1)

                yield self.connection._connection.transport.loseConnection()

                #print "%s: disconnected" % (self.load_id)

                self.cur_state['connects'] += 1

                #yield pu.asleep(1)

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
