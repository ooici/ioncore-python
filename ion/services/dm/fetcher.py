#!/usr/bin/env python

"""
@file ion/services/dm/fetcher.py
@package ion.services.dm.fetcher Remimpliment fetcher as an LCA service
@author Paul Hubbard
@date 5/7/10
@brief Porting the fetcher from DX to LCAarch.
"""

import logging

from twisted.internet import defer
from twisted.web import client

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class FetcherService(BaseService):
    """
    Fetcher, implemented as a service.

    Service declaration - seems similar to the Zope methods
    @todo Dependencies - perhaps pub-sub?
    @note These are not class methods!
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
        pass

    @defer.inlineCallbacks
    def op_get_url(self, content, headers, msg):
        self.got_err = False
        def error_callback(failure):
            """
            Inline function to catch and note getPage errors.
            """
            self.got_err = True
            self.failure = failure

        # Payload is just the URL itself
        src_url = content.encode('ascii')
        hostname = src_url.split('/')[2]

        logging.debug('Fetching page "%s"...' % src_url)

        d = client.getPage(src_url, headers={'Host': hostname})
        d.addErrback(error_callback)
        page = yield d
        if not self.got_err:
            logging.debug('Fetch complete, sending to caller')
            yield self.reply(msg, 'reply', {'value':page}, {})
            logging.debug('get_url complete!')

        # Did catch an error
        logging.error('Error on page fetch: ' + str(self.failure))
        yield self.reply(msg, 'reply',
                                 {'failure': str(self.failure),
                                'value':None})

    @defer.inlineCallbacks
    def op_get_dap_dataset(self, content, headers, msg):
        """
        The core of the fetcher: function to grab an entire DAP dataset and
        send it off into the cloud.
        """
        logging.warn('Implement me!')
        yield self.reply_err(msg, 'reply', {'value':'no code!'}, {})

class FetcherClient(BaseServiceClient):
    """
    Client class for the fetcher.
    @note RPC style interactions
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "fetcher"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def get_url(self, requested_url):
        """
        @todo look up fetcher in dns
        send to same
        return unpacked reply
        """
        yield self._check_init()

        logging.info('Sending request')
        (content, headers, msg) = yield self.rpc_send('get_url', requested_url)
        if 'failure' in content:
            raise ValueError('Error on URL: ' + content['failure'])
        defer.returnValue(content)

    def _rewrite_headers(self, old_headers):
        """
        Rewrite the message headers so that the reply-to points to the original
        sender and uses the same conversation id. Easy way to implement third-
        party messaging/coordination.
        """
        new_headers = {}
        try:
            new_headers['reply-to'] = old_headers['reply-to']
            new_headers['conv-id'] = old_headers['conv-id']
        except KeyError, ke:
            logging.exception('missing header!')
            raise ke

        return new_headers

    @defer.inlineCallbacks
    def forward_get_url(self, content, headers):
        """
        Forward a message to the fetcher.
        Reach in and rewrite the reply-to and conversation id headers, so
        that the fetcher can reply directly to the proxy and bypass the
        coordinator.
        """
        yield self.send('get_url', content, self._rewrite_headers(headers))

    @defer.inlineCallbacks
    def forward_get_dap_dataset(self, content, headers):
        """
        Same as forward_get_url, different verb.
        """
        yield self.send('get_dap_dataset', content, self._rewrite_headers(headers))

# If loaded as a module, spawn the process
factory = ProtocolFactory(FetcherService)
