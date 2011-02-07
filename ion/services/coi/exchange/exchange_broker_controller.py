import os
import warnings
  
from txamqp.content import Content
import txamqp.spec
  
from txamqp.protocol import AMQChannel, AMQClient, TwistedDelegate
  
from twisted.internet import error, protocol, reactor
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, DeferredQueue, DeferredLock
from twisted.python import failure
from txamqp.queue import Empty
  
  
RABBITMQ = "RABBITMQ"
OPENAMQ = "OPENAMQ"
QPID = "QPID"
  
  
USERNAME='guest'
PASSWORD='guest'
VHOST='/'
HEARTBEAT = 0
  
class BrokerController:
  
    clientClass = AMQClient
    heartbeat = HEARTBEAT
  
    def __init__(self, *args, **kwargs):
        self.host = 'localhost'
        self.port = 5672
        self.broker = None
        self.spec = '/Users/brianfox/workspaces/ooici/ioncore-python/ion/../res/amqp/specs/standard/amqp0-8.xml'
        self.user = USERNAME
        self.password = PASSWORD
        self.vhost = VHOST
        self.queues = []
        self.exchanges = []
        self.connectors = []


    @inlineCallbacks
    def start(self):
        """
        """
        try:
            self.client = yield self._connect()
        except txamqp.client.Closed, le:
            le.args = tuple(("Unable to connect to AMQP broker in order to run tests (perhaps due to auth failure?). " \
                "The tests assume that an instance of the %s AMQP broker is already set up and that this test script " \
                "can connect to it and use it as user '%s', password '%s', vhost '%s'." % ('',
                    USERNAME, PASSWORD, VHOST),) + le.args)
            raise
  
        self.channel = yield self.client.channel(1)
        yield self.channel.channel_open()

  
    @inlineCallbacks
    def stop(self):
        """
        """
        for ch, q in self.queues:
            yield ch.queue_delete(queue=q)
        for ch, ex in self.exchanges:
            yield ch.exchange_delete(exchange=ex)
        for connector in self.connectors:
            yield connector.disconnect()
  

  
    @inlineCallbacks
    def _connect(
            self, 
            host=None, 
            port=None, 
            spec=None, 
            user=None, 
            password=None, 
            vhost=None,
            heartbeat=None, 
            clientClass=None
    ):
        host = host or self.host
        port = port or self.port
        spec = spec or self.spec
        user = user or self.user
        password = password or self.password
        vhost = vhost or self.vhost
        heartbeat = heartbeat or self.heartbeat
        clientClass = clientClass or self.clientClass
  
        delegate = TwistedDelegate()
        onConn = Deferred()
        p = clientClass(delegate, vhost, txamqp.spec.load(spec), heartbeat=heartbeat)
        f = protocol._InstanceFactory(reactor, p, onConn)
        c = reactor.connectTCP(host, port, f)
        def errb(thefailure):
            thefailure.trap(error.ConnectionRefusedError)
            print "failed to connect to host: %s, port: %s; These tests are designed to run against a running instance" \
                  " of the %s AMQP broker on the given host and port.  failure: %r" % (host, port, self.broker, thefailure,)
            thefailure.raiseException()
        onConn.addErrback(errb)
  
        self.connectors.append(c)
        client = yield onConn
  
        yield client.authenticate(user, password)
        returnValue(client)
  
    @inlineCallbacks
    def queue_declare(self, channel=None, *args, **keys):
        channel = channel or self.channel
        reply = yield channel.queue_declare(*args, **keys)
        self.queues.append((channel, reply.queue))
        returnValue(reply)
  
    @inlineCallbacks
    def create_exchange(
                         self, 
                         channel=None, 
                         ticket=0, 
                         exchange='',
                         type='', 
                         passive=False, 
                         durable=False,
                         auto_delete=False, 
                         internal=False, 
                         nowait=False,
                         arguments={}
                                        ):
        channel = channel or self.channel
        reply = yield channel.exchange_declare(
                        ticket, 
                        exchange, 
                        type, 
                        passive, 
                        durable, 
                        auto_delete, 
                        internal, nowait, 
                        arguments
                        )
        self.exchanges.append((channel,exchange))
        returnValue(reply)
  

    @inlineCallbacks
    def consume(self, queueName):
        """Consume from named queue returns the Queue object."""
        reply = yield self.channel.basic_consume(queue=queueName, no_ack=True)
        returnValue((yield self.client.queue(reply.consumer_tag)))
  
    @inlineCallbacks
    def assertEmpty(self, queue):
        """Assert that the queue is empty"""
        try:
            yield queue.get(timeout=1)
            self.fail("Queue is not empty.")
        except Empty: None              # Ignore
  
    @inlineCallbacks
    def assertPublishGet(self, queue, exchange="", routing_key="", properties=None):
        """
        Publish to exchange and assert queue.get() returns the same message.
        """
        body = self.uniqueString()
        self.channel.basic_publish(exchange=exchange,
                                   content=Content(body, properties=properties),
                                   routing_key=routing_key)
        msg = yield queue.get(timeout=1)
        self.assertEqual(body, msg.content.body)
        if (properties): self.assertEqual(properties, msg.content.properties)
  
    def uniqueString(self):
        """Generate a unique string, unique for this TestBase instance"""
        if not "uniqueCounter" in dir(self): self.uniqueCounter = 1;
        return "Test Message " + str(self.uniqueCounter)
  
    @inlineCallbacks
    def consume(self, queueName):
        """Consume from named queue returns the Queue object."""
        reply = yield self.channel.basic_consume(queue=queueName, no_ack=True)
        returnValue((yield self.client.queue(reply.consumer_tag)))