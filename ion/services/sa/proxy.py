#!/usr/bin/env python
"""
@file ion/services/sa/proxy.py
@author Paul Hubbard
@date 5/25/10
@package ion.services.sa.proxy http->ooi proxy for user DAP access
Porting from LCO implementation to new LCA arch - complete rewrite.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.base_process import ProcessFactory
from ion.services.dm.util.eoi_data_stream_producer import CoordinatorClient

from ion.services.base_service import BaseService
from twisted.internet import defer, protocol, reactor
from twisted.protocols.basic import LineReceiver

import base64

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
    def reset(self):
        self.buf = []
        self.http_connected = False
        self.hostname = None

    def connectionMade(self):
        log.debug('connected!')
        self.reset()

    def lineReceived(self, line):
        log.debug('got a line: %s' % line)
        self.buf.append(line)
        if len(line) == 0:
            log.info('Ready to send request off!')
            self.send_receive()

    @defer.inlineCallbacks
    def send_receive(self):
        """
        Ready to send an http command off to the coordinator for dispatch.
        """
        generic_err = '500 Server error, no comprende.\r\n'
        cc = CoordinatorClient()
        # First entry in buffer should be 'GET url http/1.0' or similar
        try:
            cmd, url, method = self.buf[0].split(' ')
        except ValueError:
            log.warn('Unable to parse command "%s"' % self.buf[0])
            self.transport.write('400 Command not understood\r\n')
            self.transport.loseConnection()
            self.reset()
            return

        if 'Connection: close' in self.buf:
            # @todo Handle 1.1 persisten connections correctly
            log.debug('disconnect request found')

        if self.http_connected:
            log.debug('rewriting url...')
            url = 'http://%s%s' % (self.hostname, url)
            log.debug('new ' + url)


        if cmd == 'CONNECT':
            self.http_connected = True
            self.hostname = url
            log.debug('hostname is ' + self.hostname)

            log.debug('Ackowledging connect with fake response')
            self.transport.write('HTTP/1.1 200 Did get connection\r\n')
            self.transport.write('Content-Type: text/plain; charset=UTF-8\r\n')
            self.transport.write('\r\n')
            self.buf = []
            defer.returnValue(None)

        if cmd not in ['GET', 'HEAD']:
            log.error('Unknown command "%s"' % self.buf[0])
            yield self.transport.write(generic_err)
            yield self.transport.loseConnection()
            self.reset()
            return

        # Either get or head commands
        if cmd == 'HEAD':
            resp = yield cc.get_head(url)
        elif cmd == 'GET':
            log.debug('pulling url...')
            resp = yield cc.get_url(url)

        # Did the command succeed?
        if resp['status'] != 'OK':
            log.warn('Bad result on %s %s' % (cmd, url))
            rs = resp['value']
            log.warn(rs)
            yield self.transport.write(rs)
            yield self.transport.loseConnection()
            self.reset()
            log.debug('done with error handler')
            return

        log.debug('cmd %s returned OK' % cmd)

        # OK, response OK, decode page and return it
        self.transport.write(base64.b64decode(resp['value']))

        yield self.transport.loseConnection()
        self.reset()
        log.debug('send_receive completed')


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
        log.info('starting proxy on port %d' % PROXY_PORT)
        self.proxy_port = yield reactor.listenTCP(PROXY_PORT, DAPProxyFactory())
        log.info('Proxy listener running.')

    @defer.inlineCallbacks
    def slc_shutdown(self):
        """
        Close TCP listener
        """
        log.info('Shutting down proxy')
        yield self.proxy_port.stopListening()

factory = ProcessFactory(ProxyService)
