#!/usr/bin/env python

"""
@file ion/services/dm/fetcher.py
@package ion.services.dm.fetcher Remimpliment fetcher as an LCA service
@author Paul Hubbard
@date 5/7/10
@brief Porting the fetcher from DX to LCAarch as a learning exercise
"""

import logging

from twisted.internet import defer
from twisted.web import client

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class FetcherService(BaseService):
    """
    Fetcher, implemented as a service.
    @see FetcherService.op_get_url
    """

    """
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
        logging.debug('Service lifecycle init invoked')

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
        logging.warn('Implement me!')
        yield self.reply(msg, 'reply', {'value':'no code!'}, {})


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

#    @defer.inlineCallbacks
    def get_dap_dataset(self, requested_url, dest_address):
        """
        @todo Look up fetcher in dns
        @todo send to same
        """
        pass

# If loaded as a module, spawn the process
factory = ProtocolFactory(FetcherService)
