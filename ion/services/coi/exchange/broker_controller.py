import os
import warnings
  
from txamqp.content import Content
import txamqp.spec
  
from txamqp.protocol import AMQChannel, AMQClient, TwistedDelegate
  
from twisted.internet import error, protocol, reactor
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue

from ion import ioninit
from ion.util import ionlog

CONF = ioninit.config(__name__)
log = ionlog.getLogger(__name__)  
  
  
class BrokerController:
  

  
    def __init__(self, *args, **kwargs):
        self._privileged_broker = CONF.getValue('privileged_broker_connection')
        spec_path = os.path.join(
                os.getcwd(),
                CONF.getValue('amqp_spec')
            )
        if not os.path.isfile(spec_path):
            # the working directory is obscured by 
            # _trial_tmp on the command-line
            spec_path = os.path.join(
                os.getcwd(),
                '..',
                CONF.getValue('amqp_spec')
            )
        if not os.path.isfile(spec_path):
            log.critical('Could not locate AMQP spec file at: ' + CONF.getValue('amqp_spec'))

        self._amqp_spec = txamqp.spec.load(spec_path)
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
            log.critical(                                       
                    "failed to connect to amqp broker:\n " +    \
                    "\tusername: %s\n " +                       \
                    "\tpassword: %s\n " +                       \
                    "\thost:     %s\n " +                       \
                    "\tport:     %s\n " +                       \
                    "\tvhost:    %s" % (                        
                                str(self._privileged_broker['username']),
                                str(self._privileged_broker['password']),
                                str(self._privileged_broker['host']),
                                str(self._privileged_broker['port']),
                                str(self._privileged_broker['vhost'])))
  
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
            log.info('broker_controller: delete_exchange()  name=' + ex)
        for connector in self.connectors:
            yield connector.disconnect()
  

  
    @inlineCallbacks
    def _connect(self): 
        host = self._privileged_broker['host']
        port = self._privileged_broker['port']
        username = self._privileged_broker['username']
        password = self._privileged_broker['password']
        vhost = self._privileged_broker['vhost']
        heartbeat = self._privileged_broker['heartbeat']
  
        delegate = TwistedDelegate()
        onConn = Deferred()
        p = AMQClient(delegate, vhost, self._amqp_spec, heartbeat=heartbeat)
        f = protocol._InstanceFactory(reactor, p, onConn)
        c = reactor.connectTCP(host, port, f)
        def errb(thefailure):
            thefailure.trap(error.ConnectionRefusedError)
            log.critical(                                       
                    "failed to connect to amqp broker:\n " +    \
                    "\tusername: %s\n " +                       \
                    "\tpassword: %s\n " +                       \
                    "\thost:     %s\n " +                       \
                    "\tport:     %s\n " +                       \
                    "\tvhost:    %s" % (                        
                                str(self._privileged_broker['username']),
                                str(self._privileged_broker['password']),
                                str(self._privileged_broker['host']),
                                str(self._privileged_broker['port']),
                                str(self._privileged_broker['vhost'])))
            thefailure.raiseException()
        onConn.addErrback(errb)
  
        self.connectors.append(c)
        client = yield onConn
  
        yield client.authenticate(username, password)
        returnValue(client)
  
  
    """
    Creates an exchange.
    
    """
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
        log.info('broker_controller: create_exchange()  name=' + exchange)
        returnValue(reply)
  

    @inlineCallbacks
    def queue_declare(self, channel=None, *args, **keys):
        channel = channel or self.channel
        reply = yield channel.queue_declare(*args, **keys)
        self.queues.append((channel, reply.queue))
        returnValue(reply)


    @inlineCallbacks
    def consume(self, queueName):
        """Consume from named queue returns the Queue object."""
        reply = yield self.channel.basic_consume(queue=queueName, no_ack=True)
        returnValue((yield self.client.queue(reply.consumer_tag)))
  
