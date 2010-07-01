#!/usr/bin/env python
"""
@file ion/services/sa/proxy.py
@author Paul Hubbard
@date 5/25/10
@package ion.services.sa.proxy http->ooi proxy for user DAP access
Porting from LCO implementation to new LCA arch - complete rewrite.
"""

import logging
logging = logging.getLogger(__name__)

from ion.core.base_process import ProtocolFactory
from ion.services.dm.preservation.coordinator import CoordinatorClient

from ion.services.base_service import BaseService
from twisted.internet import defer, protocol, reactor
from twisted.protocols.basic import LineReceiver


# Read configuration file to find TCP server port
from ion.core import ioninit
config = ioninit.config(__name__)
PROXY_PORT = int(config.getValue('proxy_port', '8100'))

class DAPProxyProtocol(LineReceiver):
    """
    Super, super simple HTTP proxy. Goal: Take requests from DAP clients such
    as matlab and netcdf, convert them into OOI messages to the preservation
    service coordinator, and return whatever it gets from same as http.

    Initially written using the twisted proxy classes, but in the end what we
    need here is quite different from what the provide, so its much simpler
    to just implement it as a most-basic protocol.

    @see http://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol
    @see The DAP protocol spec at http://www.opendap.org/pdf/ESE-RFC-004v1.1.pdf
    """
    def connectionMade(self):
        logging.debug('connected!')
        self.buf = []
        self.http_connected = False
        self.hostname = None

    def lineReceived(self, line):
        logging.debug('got a line: %s' % line)
        self.buf.append(line)
        if len(line) == 0:
            logging.info('Ready to send request off!')
            self.send_receive()

    @defer.inlineCallbacks
    def send_receive(self):
        """
        Ready to send an http command off to the coordinator for dispatch.
        """
        cc = CoordinatorClient()
        # First entry in buffer should be 'GET url http/1.0' or similar
        cmd, url, method = self.buf[0].split(' ')

        if 'Connection: close' in self.buf:
            logging.debug('disconnect request found')
            do_disconnect = True
        else:
            do_disconnect = False

        if self.http_connected:
            logging.debug('rewriting url...')
            url = 'http://%s%s' % (self.hostname, url)
            logging.debug('new ' + url)

        if cmd == 'CONNECT':
            self.http_connected = True
            self.hostname = url
            logging.debug('hostname is ' + self.hostname)

            logging.debug('Ackowledging connect with fake response')
            self.transport.write('HTTP/1.1 200 Did get connection\r\n')
            self.transport.write('Content-Type: text/plain; charset=UTF-8\r\n')
            self.transport.write('\r\n')
            self.buf = []
        elif cmd == 'HEAD':
            page = yield cc.get_head(url)
            self.transport.write(page)
        elif cmd == 'GET':
            logging.debug('pulling url...')
            page = yield cc.get_url(url)
            self.transport.write(page)
            yield self.transport.loseConnection()
        else:
            logging.error('Unknown command "%s"' % self.buf[0])
            self.transport.write('HTTP/1.0 500 Server error, no comprende.\r\n')

        if do_disconnect:
            yield self.transport.loseConnection()

        self.buf = []


class DAPProxyFactory(protocol.ServerFactory):
    protocol = DAPProxyProtocol

class ProxyService(BaseService):
    """
    Proxy service. Stub, really, since the proxy listens on a plain tcp port.
    """
    # Declaration of service
    declare = BaseService.service_declare(name='proxy',
                                          version='0.1.0',
                                          dependencies=['controller'])

    @defer.inlineCallbacks
    def slc_init(self):
        """
        Use this hook to bind to listener TCP port.
        """
        logging.info('starting proxy on port %d' % PROXY_PORT)
        self.proxy_port = yield reactor.listenTCP(PROXY_PORT, DAPProxyFactory())
        logging.info('Proxy listener running.')

    @defer.inlineCallbacks
    def slc_shutdown(self):
        """
        Close TCP listener
        """
        logging.info('Shutting down proxy')
        yield self.proxy_port.stopListening()

factory = ProtocolFactory(ProxyService)
