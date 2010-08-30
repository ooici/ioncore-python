#!/usr/bin/env python


"""
@file ion/services/dm/presentation/twitter_consumer.py
@author David Stuebe
@brief Web Visualization for PubSub Data - see here to make a google viz chart
http://code.google.com/apis/visualization/documentation/index.html
@Note Example in the message_count_consumer.py
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

from twisted.internet import defer, reactor
from twisted.web import server, resource

import logging
log = logging.getLogger(__name__)


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

        log.info('Setting up webserver...')
        
        port = self.params.get('port',2100)
        
        self.port = None
        self.root = DMUI()
        self.site = server.Site(self.root)
        self.port = reactor.listenTCP(port, self.site)
        self.root.print_string = None
        
        self.values=[]
        log.info('Website started')


    @defer.inlineCallbacks
    def plc_shutdown(self):
        log.info('Shutdown triggered')
        if self.port:
            yield self.port.stopListening()

    #@defer.inlineCallbacks
    def ondata(self, data, notification, timestamp, **args):
        """
        """
        log.info('Updating google viz chart data!')
        
        if isinstance(data,str):
            self.root.print_string = data
        else:
            raise RuntimeError('Invalid data (Not a String) passed to WebVizConsumer ondata method!')
            
        log.info('Update complete!')

# Spawn of the process using the module name
factory = ProtocolFactory(WebVizConsumer)
