#!/usr/bin/env python
"""
@file ion/services/dm/coordinator.py
@author Paul Hubbard
@date 5/25/10
@package ion.services.dm.coordinator DX/DM coordinator/orchestrator. The brains.

Rewrite from LCO version; refactor/rewrite for new LCA arch.
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class CoordinatorService(BaseService):
    """
    Brains behind DX, and also the primary external interface.
    """
    # Define ourselves for the CC
    declare = BaseService.service_declare(name='coordinator',
                                          version='0.1.0',
                                          dependencies=[])

    def __init__(self, receiver, spawnArgs=None):
        BaseService.__init__(self, receiver, spawnArgs)
        logging.debug('CoordinatorService.__init__()')

    def slc_init(self):
        """
        Service life cycle state. Initialize service here. Can use yields.
        """
        logging.debug('CoordinatorService SLC init')

    @defer.inlineCallbacks
    def op_get_url(self, content, headers, msg):
        """
        @brief Method for proxy - request a (DAP) URL
        @param content ?
        @param headers ?
        @param msg ?
        @retval Return message with http payload or error
        @note RPC message pattern for this one?
        """
        logging.error('Implement me!')
        yield self.reply_err(msg, {'value': 'Not implemented '}, {})

class CoordinatorClient(BaseServiceClient):
    """
    Caller interface to coordinator.
    @see ion.services.dm.proxy for an example
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'coordinator'
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def get_url(self, url):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_url', url)
        logging.info('Reply from service: '+ str(content))
        defer.returnValue(str(content))


factory = ProtocolFactory(CoordinatorService)
