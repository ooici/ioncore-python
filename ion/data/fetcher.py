#!/usr/bin/env python

"""
@file ion/data/fetcher.py
@author Paul Hubbard
@date 5/7/10
@brief Porting the fetcher from DX to LCAarch as a learning exercise
"""

import logging

from twisted.internet import defer

from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService

class FetcherService(BaseService):
    """
    Fetcher, implemented as a service.
    @see FetcherService.op_get_url
    """

    """
    Service declaration - seems similar to the Zope methods
    @todo Dependencies - perhaps pub-sub?
    """
    declare = BaseService.service_declare(name='fetcher',
                                          version='1.0.0',
                                          dependencies='[]')

    def __init__(self, receiver, spawnArgs=None):
        BaseService.__init__(self, receiver, spawnArgs)
        logging.debug('Fetcher starting')

    def slc_init(self):
        logging.debug('Service lifecycle init invoked')

#    @defer.inlineCallbacks
    def op_get_url(self, content, headers, msg):
        logging.warn('Implement me!')

#    @defer.inlineCallbacks
    def op_get_dap_dataset(self, content, headers, msg):
        logging.warn('Implement me!')


class FetcherClient(RpcClient):
    """
    Client class for the fetcher.
    @note RPC style interactions
    """

#    @defer.inlineCallbacks
    def get_url(self, requested_url):
        """
        look up fetcher in dns
        send to same
        return unpacked reply
        """
        pass

#    @defer.inlineCallbacks
    def get_dap_dataset(self, requested_url, dest_address):
        """
        Look up fetcher in dns
        send to same
        """
        pass

# If loaded as a module, spawn the process
factory = ProtocolFactory(FetcherService)
