#!/usr/bin/env python


"""
@file ion/services/dm/presentation/twitter_consumer.py
@author David Stuebe
@brief twitter the notification component of the data!
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

from twisted.internet import defer, reactor
from twisted.web import server, resource

import logging
logging = logging.getLogger(__name__)


class DMUI(resource.Resource):
    isLeaf = True

    def render_GET(self, request):
        if not self.print_string:
            return 'LCA Arch Web Viz page!\n No data yet...'
        else:
            return str(self.print_string)


class WebVizConsumer(base_consumer.BaseConsumer):
    """
    Publish data to a web vis page!
    """
    #@defer.inlineCallbacks
    def customize_consumer(self):

        logging.info('Setting up webserver...')
        
        port = self.params.get('port',2100)
        
        self.port = None
        self.root = DMUI()
        self.site = server.Site(self.root)
        self.port = reactor.listenTCP(port, self.site)
        self.root.print_string = None
        
        self.values=[]
        logging.info('Website started')


    @defer.inlineCallbacks
    def plc_shutdown(self):
        logging.info('Shutdown triggered')
        if self.port:
            yield self.port.stopListening()

    #@defer.inlineCallbacks
    def ondata(self, data, notification, timestamp, **args):
        """
        """
        logging.info('Updating google viz chart data!')
        
        if isinstance(data,str):
            self.root.print_string = data
        else:
            raise RuntimeError('Invalid data (Not a String) passed to WebVizConsumer ondata method!')
            
        logging.info('Update complete!')

# Spawn of the process using the module name
factory = ProtocolFactory(WebVizConsumer)
