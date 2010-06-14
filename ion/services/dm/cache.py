#!/usr/bin/env python

"""
@file ion/services/dm/cache.py
@author Paul Hubbard
@date 6/11/10
@brief Cache - front end to local DAP server that sends DAP data out over OOI.
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.services.dm.url_manipulation import rewrite_url
from ion.services.sa.fetcher import FetcherService

class CacheService(FetcherService):
    """
    Cache - subclass of ion.services.sa.fetcher
    OOI front end for a local DAP server.
    """
    declare = BaseService.service_declare(name='cache',
                                  version='0.0.1',
                                  dependencies=[])

    def op_get_url(self, content, headers, msg):
        """
        Rewrite the URL, forward the request.
        """
        src_url = content
        new_url = rewrite_url(src_url)

        logging.debug('Old url: %s New url: %s' % (src_url, new_url))
        return self._http_op('GET', new_url, msg)

class CacheClient(BaseServiceClient):
    """
    Client interface to the cache.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'cache'
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def get_url(self, url):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_url', url)
        defer.returnValue(content)

factory = ProtocolFactory(CacheService)
