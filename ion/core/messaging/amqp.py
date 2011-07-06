"""
AMQP Messaging Client Protocol 

These are extensions and fixes to the txamqp library
"""

import os
from time import time

from twisted.internet import defer
from twisted.internet import protocol
from twisted.python import failure

from txamqp import spec
from txamqp.content import Content
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient, AMQChannel, Frame
from txamqp.queue import TimeoutDeferredQueue

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from ion.core import messaging
SPEC_PATH = os.path.join(messaging.__path__[0], 'amqp0-8.xml')

class IDPool(object):
    """
    Create a pool of IDs to allow reuse. The "new_id" function generates the next
    valid ID from the previous one. If not given, defaults to incrementing an integer.
    """

    def __init__(self, new_id=None):
        if new_id is None: new_id = lambda x: x + 1

        self.ids_in_use = set()
        self.ids_free = set()
        self.new_id = new_id
        self.last_id = 0

    def get_id(self):
        if len(self.ids_free) > 0:
            return self.ids_free.pop()

        self.last_id = id_ = self.new_id(self.last_id)
        self.ids_in_use.add(id_)
        return id_

    def release_id(self, the_id):
        if the_id in self.ids_in_use:
            self.ids_in_use.remove(the_id)
            self.ids_free.add(the_id)


class HeartbeatExpired(Exception):
    """
    """

class ChannelWithCallback(AMQChannel):

    def __init__(self, id, outgoing, client):
        AMQChannel.__init__(self, id, outgoing)
        self.deliver_callback = lambda dev_null: dev_null
        self.client = client
    
    def set_consumer_callback(self, callback):
        self.deliver_callback = callback

    def close(self, reason):
        AMQChannel.close(self, reason)
        self.client._channel_closed(self.id)

    def channel_close(self):
        def close_cb(r):
            self.close(r)
            return r
        d = super(ChannelWithCallback, self).channel_close()
        d.addCallback(close_cb)
        return d


class AMQPProtocol(AMQClient):
    """ Improvements to the txamqp implementation.
    """

    channelClass=ChannelWithCallback
    #next_channel_id = 0
    closed = True

    def __init__(self, *args, **kwargs):
        AMQClient.__init__(self, *args, **kwargs)
        self.id_pool = IDPool()

    def channel(self, id=None):
        """Overrides AMQClient. Changes: 
            1) no need to return deferred. The channelLock doesn't protect
            against any race conditions; the channel reference is returned,
            so any number of those references could exist already. 
            2) auto channel numbering
            3) replace deferred queue for basic_deliver(s) with simple
               buffer(list)
        """
        if id is None:
            #self.next_channel_id += 1
            #id = self.next_channel_id
            id = self.id_pool.get_id()
        try:
            ch = self.channels[id]
        except KeyError:
            ch = self.channelFactory(id, self.outgoing, self)
            self.channels[id] = ch
        return ch

    def _channel_closed(self, id):
        if self.channels.has_key(id):
            del self.channels[id]
            self.id_pool.release_id(id)

    def queue(self, key):
        """ This is the channel basic_deliver queue
        """
        try:
            q = self.queues[key]
        except KeyError:
            q = TimeoutDeferredQueue()
            self.queues[key] = q
        return q

    def connectionMade(self):
        self.closed = False
        AMQClient.connectionMade(self)

    def processFrame(self, frame):
        ch = self.channel(frame.channel)
        if frame.payload.type == Frame.HEARTBEAT:
            self.lastHBReceived = time()
        else:
            ch.dispatch(frame, self.work)
        if self.heartbeatInterval > 0:
            self.reschedule_checkHB()

    def frameLengthExceeded(self):
        """
        """

    def checkHeartbeat(self):
        if self.checkHB.active():
            self.checkHB.cancel()
        log.critical('AMQP Heartbeat expired on client side')
        self.transport.loseConnection(failure.Failure(HeartbeatExpired('Heartbeat expired.')))

 
class ConnectionCreator(object):
    """Create AMQP Client.
    The AMQP Client uses one persistent connection, so a Factory is not
    necessary.

    Client Creator is initialized with AMQP Broker configuration.

    ConnectTCP is called with TCP configuration.
    """

    protocol = AMQPProtocol
    spec_cache = {}

    def __init__(self, reactor, username='guest', password='guest',
                                vhost='/', delegate=None,
                                spec_path=SPEC_PATH,
                                heartbeat=0):
        self.reactor = reactor
        self.username = username
        self.password = password
        self.vhost = vhost

        # Cache the specs for enormous speedups on subsequent connections
        cache = ConnectionCreator.spec_cache
        if spec_path in cache:
            self.spec = cache[spec_path]
        else:
            self.spec = cache.setdefault(spec_path, spec.load(spec_path))

        self.heartbeat = heartbeat
        if delegate is None:
            delegate = TwistedDelegate()
        self.delegate = delegate
        self.connector = None

    def connectTCP(self, host, port, timeout=30, bindAddress=None):
        """Connect to remote Broker host, return a Deferred of resulting protocol
        instance.
        """
        d = defer.Deferred()
        p = self.protocol(self.delegate,
                                    self.vhost,
                                    self.spec,
                                    heartbeat=self.heartbeat)
        p.factory = self
        f = protocol._InstanceFactory(self.reactor, p, d)
        self.connector = self.reactor.connectTCP(host, port, f, timeout=timeout,
                bindAddress=bindAddress)
        def auth_cb(conn):
            d = conn.authenticate(self.username, self.password)
            d.addCallback(lambda _: conn)
            return d
        d.addCallback(auth_cb)
        return d



