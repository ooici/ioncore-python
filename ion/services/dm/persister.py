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
logging = logging.getLogger(__name__)

import time
import sys

from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.dm.url_manipulation import generate_filename

class PersisterService(BaseService):
    declare = BaseService.service_declare(name='persister',
                                          version='0.1.0',
                                          dependencies=[])
    """
    @note Relies on a single message fitting in memory comfortably.
    @todo Depend on pub-sub
    """
    @defer.inlineCallbacks
    def op_persist_dap_dataset(self, content, headers, msg):
        logging.info('called to persist a dap dataset!')

        assert(isinstance(content, dict))

        try:
            rc = self._save_no_xmit(content)
        except KeyError:
            yield self.reply_err(msg, {'value':'Missing headers'}, {})
            return
        if rc:
            yield self.reply_err(msg, {'value': 'Error saving!'}, {})

        yield self.reply_ok(msg)

    def _save_no_xmit(self, content, local_dir=None):
        try:
            dds = json.loads(content['dds'])
            das = json.loads(content['das'])
            dods = content['value']
            source_url = content['source_url']
        except KeyError, ke:
            logging.error('Unable to find required fields in dataset!')
            raise ke

        logging.debug('DAS snippet: ' + das[:80])
        logging.debug('DDS snippet: ' + dds[:80])

        return(self._save_dataset(das, dds, dods, source_url, local_dir=local_dir))

    def _save_dataset(self, das, dds, dods, source_url, local_dir=None):
        dataset = DDSParser(dds).parse()
        dataset = DASParser(das, dataset).parse()

        """
        Tag global attributes with cache info
        @todo Design decision - what goes into per-file metadata?
        @note This is purely OOI code - not pydap at all.
        """
        dataset.attributes['NC_GLOBAL']['ooi-download-timestamp'] = time.time()
        dataset.attributes['NC_GLOBAL']['ooi-source-url'] = source_url

        """
        Back to pydap code - this block is from open_url in client.py
        Remove any projections from the url, leaving selections.
        """
        scheme, netloc, path, query, fragment = urlsplit(source_url)
        projection, selection = parse_qs(query)
        url = urlunsplit(
                (scheme, netloc, path, '&'.join(selection), fragment))

        # Set data to a Proxy object for BaseType and SequenceType. These
        # variables can then be sliced to retrieve the data on-the-fly.
        for var in walk(dataset, BaseType):
            var.data = ArrayProxy(var.id, url, var.shape)
        for var in walk(dataset, SequenceType):
            var.data = SequenceProxy(var.id, url)

        # Apply the corresponding slices.
        projection = fix_shn(projection, dataset)
        for var in projection:
            target = dataset
            while var:
                token, slice_ = var.pop(0)
                target = target[token]
                if slice_ and isinstance(target.data, VariableProxy):
                    shape = getattr(target, 'shape', (sys.maxint,))
                    target.data._slice = fix_slice(slice_, shape)

        # This block is from open_dods in client.py
        dds, xdrdata = dods.split('\nData:\n', 1)
        dataset.data = DapUnpacker(xdrdata, dataset).getvalue()

        fname = generate_filename(source_url, local_dir=local_dir)
        logging.info('Saving DAP dataset "%s" to "%s"' % (source_url, fname))

        netcdf.save(dataset, fname)

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
