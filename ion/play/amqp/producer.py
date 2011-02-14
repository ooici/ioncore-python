import sys

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

import common


@inlineCallbacks
def pushText(chan, body):
    msg = Content(body)
    # we don't want to see these test messages every time the consumer connects
    # to the RabbitMQ server, so we opt for non-persistent delivery
    msg["delivery mode"] = common.NON_PERSISTENT
    # publiish the message to our exchange; use the routing key to decide which
    # queue the exchange should send it to
    yield chan.basic_publish(
        exchange=common.EXCHANGE_NAME, content=msg,
        routing_key=common.ROUTING_KEY)
    returnValue(None)


@inlineCallbacks
def cleanUp(conn, chan):
    yield chan.channel_close()
    # the AMQP spec says that connection/channel closes should be done
    # carefully; the txamqp.protocol.AMQPClient creates an initial channel with
    # id 0 when it first starts; we get this channel so that we can close it
    chan = yield conn.channel(0)
    # close the virtual connection (channel)
    yield chan.connection_close()
    reactor.stop()
    returnValue(None)


@inlineCallbacks
def main(spec):
    delegate = TwistedDelegate()
    # create the Twisted producer client
    producer = ClientCreator(
        reactor, AMQClient, delegate=delegate,
        vhost=common.VHOST, spec=spec)
    # connect to the RabbitMQ server
    conn = yield common.getConnection(producer)
    # get the channel
    chan = yield common.getChannel(conn)
    # send the text to the RabbitMQ server
    yield pushText(chan, sys.argv[2])
    # shut everything down
    yield cleanUp(conn, chan)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "%s path_to_spec content" % sys.argv[0]
        sys.exit(1)
    spec = txamqp.spec.load(sys.argv[1])
    main(spec)
    reactor.run()
