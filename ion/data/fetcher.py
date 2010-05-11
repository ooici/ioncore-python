#!/usr/bin/env python

"""
@file ion/data/fetcher.py
@author Paul Hubbard
@date 5/7/10
@brief Porting the fetcher from DX to LCAarch as a learning exercise
"""

import logging

from twisted.internet import defer
from twisted.web import client

from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService
from ion.services.coi.service_registry import ServiceRegistryClient

class FetcherService(BaseService):
    """
    Fetcher, implemented as a service.
    @see FetcherService.op_get_url
    """

    """
    Service declaration - seems similar to the Zope methods
    @todo Dependencies - perhaps pub-sub?
    """
    logging.info('Declaring fetcher...')
    declare = BaseService.service_declare(name='fetcher',
                                          version='0.1.0',
                                          dependencies=[])
    """
    @todo Declare fetcher name into dns-equivalent...
    """

    def __init__(self, receiver, spawnArgs=None):
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('Fetcher starting')

    def slc_init(self):
        logging.debug('Service lifecycle init invoked')

    @defer.inlineCallbacks
    def op_get_url(self, content, headers, msg):
        # Payload is just the URL itself
        src_url = content.encode('ascii')
        hostname = src_url.split('/')[2]

        logging.debug('Fetching page "%s"...' % src_url)
        page = yield client.getPage(src_url, headers={'Host': hostname})
        logging.debug('Fetch complete, sending to caller')
        yield self.reply_message(msg, 'reply', {'value':page}, {})
        logging.debug('get_url complete!')

    @defer.inlineCallbacks
    def op_get_dap_dataset(self, content, headers, msg):
        logging.warn('Implement me!')
        yield self.reply_message(msg, 'reply', {'value':'no code!'}, {})


class FetcherClient(RpcClient):
    """
    Client class for the fetcher.
    @note RPC style interactions
    """

    @defer.inlineCallbacks
    def get_url(self, faddr, requested_url):
        """
        @todo look up fetcher in dns
        send to same
        return unpacked reply
        """
#        logging.info('Lookup up the service ID...')
#        sc = ServiceRegistryClient()
#        dest = '1'
        #yield sc.get_service_instance('fetcher')

        logging.info('Sending request')
        (content, headers, msg) = yield self.rpc_send(faddr, 'get_url',
                                                      requested_url, {})
        defer.returnValue(content)

#    @defer.inlineCallbacks
    def get_dap_dataset(self, requested_url, dest_address):
        """
        @todo Look up fetcher in dns
        @todo send to same
        """
        pass

# If loaded as a module, spawn the process
factory = ProtocolFactory(FetcherService)
