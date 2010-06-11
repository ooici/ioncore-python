#!/usr/bin/env python

"""
@file ion/services/dm/ingest.py
@author Michael Meisinger
@brief service for ingesting information into DM
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from pydap.parsers.dds import DDSParser
from pydap.parsers.das import DASParser
import simplejson as json

class IngestService(BaseService):
    """Ingestion service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='ingest',
                                          version='0.1.0',
                                          dependencies=[])

    def op_ingest_dap(self, content, headers, msg):
        """
        Given a DAP message, pull out a dictionary of attributes
        via pydap calls on the metadata.
        @note Just returns the dictionary of attributes and pydap objects
        """
        dataset = self._do_ingest(content)
        self.reply_ok(msg, dataset, {})

    def _do_ingest(self, content):
        """
        Does the work but not the messaging.
        """
        try:
            dds = json.loads(content['dds'])
            das = json.loads(content['das'])
        except KeyError, ke:
            logging.exception(ke)
            logging.error('Unable to find headers in DAP message!')
            raise ke

        dataset = DDSParser(dds).parse()
        dataset = DASParser(das, dataset).parse()
        return dataset

class IngestClient(BaseServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'ingest'
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def ingest_dap_dataset(self, dap_dataset):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('ingest_dap', dap_dataset)
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProtocolFactory(IngestService)
