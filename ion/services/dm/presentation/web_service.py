#!/usr/bin/env python

"""
@file ion/services/dm/presentation/web_service.py
@author Paul Hubbard
@brief HTTP Server for DM presentation services
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer, reactor
from twisted.web import server, resource

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class DMUI(resource.Resource):
    isLeaf = True

    def render_GET(self, request):
        if not self.print_string:
            return 'LCA Arch web page!'
        else:
            return str(self.print_string)

class WebService(BaseService):
    """
    Open a http server inside an LCA service
    """
    # Declaration of service
    declare = BaseService.service_declare(name='web',
                                          version='0.0.1',
                                          dependencies=[])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        logging.info('Setting up webserver...')
        self.root = DMUI()
        self.site = server.Site(self.root)
        self.port = reactor.listenTCP(2100, self.site)
        self.root.print_string = None
        logging.info('Website started')

    @defer.inlineCallbacks
    def slc_shutdown(self):
        logging.info('Shutdown triggered')
        yield self.port.stopListening()

    @defer.inlineCallbacks
    def op_set_string(self, content, headers, msg):
        logging.info('op_set_string: '+str(content))

        self.root.print_string = content

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Done OK! '+str(content)}, {})


class WebServiceClient(BaseServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "web"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def set_string(self, text='Hi there'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('set_string', text)
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProtocolFactory(WebService)



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
