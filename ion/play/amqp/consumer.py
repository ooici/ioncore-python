import sys

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
import txamqp.spec

import common


@inlineCallbacks
def getQueue(conn, chan):
    # in order to get the queue, we've got some setup to do; keep in mind that
    # we're not interested in persisting messages
    #
    # create an exchange on the message server
    yield chan.exchange_declare(
        exchange=common.EXCHANGE_NAME, type="direct",
        durable=False, auto_delete=True)
    # create a message queue on the message server
    yield chan.queue_declare(
        queue=common.QUEUE_NAME, durable=False, exclusive=False,
        auto_delete=True)
    # bind the exchange and the message queue
    yield chan.queue_bind(
        queue=common.QUEUE_NAME, exchange=common.EXCHANGE_NAME,
        routing_key=common.ROUTING_KEY)
    # we're writing a consumer, so we need to create a consumer, identifying
    # which queue this consumer is reading from; we give it a tag so that we
    # can refer to it later
    yield chan.basic_consume(
        queue=common.QUEUE_NAME,
        consumer_tag=common.CONSUMER_TAG)
    # get the queue that's associated with our consumer
    queue = yield conn.queue(common.CONSUMER_TAG)
    returnValue(queue)


@inlineCallbacks
def processMessage(chan, queue):
    msg = yield queue.get()
    print "Received: %s from channel #%s" % (
        msg.content.body, chan.id)
    processMessage(chan, queue)
    returnValue(None)


@inlineCallbacks
def main(spec):
    delegate = TwistedDelegate()
    # create the Twisted consumer client
    consumer = ClientCreator(
        reactor, AMQClient, delegate=delegate,
        vhost=common.VHOST, spec=spec)
    # connect to the RabbitMQ server
    conn = yield common.getConnection(consumer)
    # get the channel
    chan = yield common.getChannel(conn)
    # get the message queue
    queue = yield getQueue(conn, chan)
    while True:
        yield processMessage(chan, queue)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "%s path_to_spec" % sys.argv[0]
        sys.exit(1)
    spec = txamqp.spec.load(sys.argv[1])
    main(spec)
    reactor.run()
