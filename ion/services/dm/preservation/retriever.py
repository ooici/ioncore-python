#!/usr/bin/env python

"""
@file ion/services/dm/preservation/retriever.py
@package ion.services.dm.preservation.retriever Netcdf->DAP front end
@author Paul Hubbard
@date 6/11/10
@brief Retriever - front end to local DAP server that sends DAP data out over OOI.
"""

import logging
log = logging.getLogger(__name__)

from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.services.dm.util.url_manipulation import rewrite_url
from ion.services.sa.fetcher import FetcherService

class RetrieverService(FetcherService):
    """
    Preservation service retriever - subclass of ion.services.sa.fetcher

    OOI front end for a local DAP server. Uses DAP to talk to locally-running
    instance of the DAP server, which is responsible for netcdf->dap
    transformation. Any dap server should work that can present netcdf files;
    we often use pydap for testing.

    Because we rely on the DAP server to do that transformation, this can be
    a thin layer of code.

    @see ion.services.dm.url_manipulation for the rewrite_url routine and its
    unit tests.
    """
    declare = BaseService.service_declare(name='retriever',
                                  version='0.0.2',
                                  dependencies=[])

    def op_get_url(self, content, headers, msg):
        """
        Rewrite the URL, forward the request to the DAP server
        """
        src_url = content
        new_url = rewrite_url(src_url)

        log.debug('Old url: %s New url: %s' % (src_url, new_url))
        # Note that _http_op is inherited fetcher code...
        return self._http_op('GET', new_url, msg)

class RetrieverClient(BaseServiceClient):
    """
    Client interface to the retriever.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'retriever'
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def get_url(self, url):
        """
        The interface is just a collection of DAP URLs.
        """

        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_url', url)
        defer.returnValue(content)

factory = ProtocolFactory(RetrieverService)
