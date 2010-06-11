#!/usr/bin/env python

"""
@file ion/services/sa/tinyproxy.py
@package ion.services.sa.proxy Build proxy on raw sockets
@author SUZUKI Hisao
@date 5/28/10
@see http://www.okisoft.co.jp/esc/python/proxy/
@see http://docs.python.org/library/basehttpserver.html
@note Modifications by Paul Hubbard to change into http<->AMQP proxy.

Copyright (c) 2001 SUZUKI Hisao

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

"""
__version__ = '0.1.0'

import BaseHTTPServer, socket, SocketServer, urlparse

import logging
logging = logging.getLogger(__name__)
from ion.services.dm.coordinator import CoordinatorClient
from twisted.internet import defer

class ProxyHandler (BaseHTTPServer.BaseHTTPRequestHandler):
    __base = BaseHTTPServer.BaseHTTPRequestHandler
    __base_handle = __base.handle

    server_version = "tinyproxy/" + __version__
    rbufsize = 0                        # self.rfile Be unbuffered

    def do_CONNECT(self):
        logging.debug('connect method firing')
        self.log_request(200)
        self.wfile.write(self.protocol_version +
                         " 200 Connection established\r\n")
        self.wfile.write("Proxy-agent: %s\r\n" % self.version_string())
        self.wfile.write("\r\n")

    @defer.inlineCallbacks
    def do_GET(self):
        (scm, netloc, path, params, query, fragment) = urlparse.urlparse(
            self.path, 'http')
        if scm != 'http' or fragment or not netloc:
            self.send_error(400, "bad url %s" % self.path)
            return

        cc = CoordinatorClient()
        logging.debug('Sending URL request to coordinator...')
        msg = yield cc.get_url(self.path)
        self.connection.send(msg)

    @defer.inlineCallbacks
    def do_HEAD(self):
        (scm, netloc, path, params, query, fragment) = urlparse.urlparse(
            self.path, 'http')
        if scm != 'http' or fragment or not netloc:
            self.send_error(400, "bad url %s" % self.path)
            return

        cc = CoordinatorClient()
        msg = yield cc.get_head(self.path)
        self.connection.send(msg)

    """
    @note Perhaps mark some of these as None instead?
    """
#    do_POST = do_GET
#    do_PUT  = do_GET
#    do_DELETE=do_GET

class ThreadingHTTPServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):
    pass

if __name__ == '__main__':
    from sys import argv
    if argv[1:] and argv[1] in ('-h', '--help'):
        print argv[0], "[port [allowed_client_name ...]]"
    else:
        if argv[2:]:
            allowed = []
            for name in argv[2:]:
                client = socket.gethostbyname(name)
                allowed.append(client)
                print "Accept: %s (%s)" % (client, name)
            ProxyHandler.allowed_clients = allowed
            del argv[2:]
        else:
            print "Any clients will be served..."
        BaseHTTPServer.test(ProxyHandler, ThreadingHTTPServer)
