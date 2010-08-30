#!/usr/bin/env python
"""
@file ion/services/dm/util/eoi_data_stream_producer.py
@author Paul Hubbard
@date 5/25/10
@package ion.services.dm.preservation.coordinator Preservation coordinator. The brains.

Rewrite from LCO version; refactor/rewrite for new LCA arch.
"""

import logging
log = logging.getLogger(__name__)
from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.sa.fetcher import FetcherClient

class CoordinatorService(BaseService): 
    """
    Refactor this into a BaseService that provides dap data on a looping call
    
    Make the url a parameter of the process - one per url...
    
    Brains behind preservation, and also the primary interface.
    """
    # Define ourselves for the CC
    declare = BaseService.service_declare(name='coordinator',
                                          version='0.1.0',
                                          dependencies=['fetcher'])

    def slc_init(self):
        """
        Service life cycle state. Initialize service here. Can use yields.
        @todo Create instances of clients here for later - fetcher, attr store, etc
        """
        log.debug('Preservation coordinator SLC init')
        self.fc = FetcherClient(proc=self)

    @defer.inlineCallbacks
    def op_get_url(self, content, headers, msg):
        """
        @brief Method for proxy - request a (DAP) URL
        @param content URL to fetch
        @param headers conv-id and reply-to should point to proxy/requester
        @param msg Not used
        @todo Cache logic - right now just trapdoors all reqs to fetcher
        """
        log.debug('Coordinator forwarding URL request to fetcher')
        yield self.fc.forward_get_url(content, headers)

    @defer.inlineCallbacks
    def op_get_dap_dataset(self, content, headers, msg):
        """
        @brief Similar to op_get_url. Fetches an entire DAP dataset.
        @param content URL to fetch
        @param headers conv-id and reply-to should point to proxy/requester
        @param msg Not used
        @todo Cache logic - right now just trapdoors all reqs to fetcher
        """
        yield self.fc.forward_get_dap_dataset(content, headers)


class CoordinatorClient(BaseServiceClient):
    """
    Caller interface to coordinator.
    @see ion.services.sa.proxy for an example
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'coordinator'
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def get_url(self, url):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_url', url)
        # @bug get_url returns unicode, must cast to string or transport.write barfs
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_head(self, url):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_head', url)
        #log.info('Reply from service: '+ content['value'])
        defer.returnValue(content)

factory = ProtocolFactory(CoordinatorService)
