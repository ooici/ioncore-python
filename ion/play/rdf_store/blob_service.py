#!/usr/bin/env python

"""
@file ion/play/rdf_store/blob_service.py
@author David Stuebe
@brief  RDF Store: blob service
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.data.dataobject import DataObject
from ion.data.store import Store
from ion.data.objstore import ValueObject

class BlobService(BaseService):
    """Example service implementation
    """

    # Declaration of service
    declare = BaseService.service_declare(name='blobs', version='0.1.0', dependencies=[])

#    def __init__(self, receiver, spawnArgs=None):
#        BaseService.__init__(self, receiver, spawnArgs)
#        logging.info('HelloService.__init__()')

    def slc_init(self):
        self.store = Store()

    @defer.inlineCallbacks
    def op_put_blob(self, content, headers, msg):
        '''
        content is a DataObject encoding - a Dictionary!
        For storing blobs, we really just want to store it encoded.
        Can we get the serialized value from the messaging layer?
        '''
        logging.info('op_put_blob: '+str(content))
        blob = ValueObject(content)

        yield self.store.put(blob.identity, blob.value)

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', {'Stored Key':blob.identity}, {})

    @defer.inlineCallbacks
    def op_get_blob(self, content, headers, msg):
        '''

        '''

        blob = yield self.store.get(content['key'])

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', blob, {})

    @defer.inlineCallbacks
    def op_del_blob(self, content, headers, msg):
        '''

        '''

        yield self.store.delete(content['key'])

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', {'result':'success'}, {})


class BlobServiceClient(BaseServiceClient):
    """
    This is an exemplar service class that calls the hello service. It
    applies the RPC pattern.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "blobs"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def put_blob(self, blob):
        '''
        @param blob A DataObject blob to be stored
        '''
        yield self._check_init()

        assert isinstance(blob, DataObject)

        (content, headers, msg) = yield self.rpc_send('put_blob', blob.encode())
        logging.info('Blob Servie Client: put_blob: '+str(content))
        defer.returnValue(content['Stored Key'])

    @defer.inlineCallbacks
    def get_blob(self, key):
        '''
        @param key, A key for a stored blob
        '''
        yield self._check_init()

        # assert ?
        kd={'key':key}
        (content, headers, msg) = yield self.rpc_send('get_blob', kd)
        logging.info('Blob Servie Client: get_blob: '+str(content))
        blob=DataObject.from_encoding(content)
        defer.returnValue(blob)


    @defer.inlineCallbacks
    def del_blob(self, key):
        '''
        @param key, A key for a stored blob
        '''
        yield self._check_init()

        # assert ?
        kd={'key':key}
        (content, headers, msg) = yield self.rpc_send('del_blob', kd)
        logging.info('Blob Servie Client: del_blob: '+str(content))
        defer.returnValue(content['result'])

# Spawn of the process using the module name
factory = ProtocolFactory(BlobService)
