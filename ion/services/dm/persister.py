#!/usr/bin/env python

"""
@file ion/services/dm/persister.py
@author Paul Hubbard
@date 6/7/10
@brief The persister writes DAP datasets to disk as netcdf files.
"""


from urlparse import urlsplit, urlunsplit
import simplejson as json

from pydap.model import BaseType, SequenceType
from pydap.proxy import ArrayProxy, SequenceProxy, VariableProxy
from pydap.parsers.dds import DDSParser
from pydap.parsers.das import DASParser
from pydap.xdr import DapUnpacker
from pydap.lib import walk, fix_slice, parse_qs, fix_shn
from pydap.responses import netcdf

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class PersisterService(BaseService):
    declare = BaseService.service_declare(name='persister',
                                          version='0.1.0',
                                          dependencies=[])
    """
    @todo Depend on pub-sub
    """

    @defer.inlineCallbacks
    def op_persist_dap_dataset(self, content, headers, msg):
        logging.info('called to persist a dap dataset!')
        yield self.reply_err(msg, {'value': 'No code yet!'}, {})


class PersisterClient(BaseServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'persister'
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def persist_dap_dataset(self, dap_message):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('persist_dap_dataset',
                                                      dap_message)
        logging.debug('dap persist returns: ' + str(content))
        defer.returnValue(str(content))
        
factory = ProtocolFactory(PersisterService)
