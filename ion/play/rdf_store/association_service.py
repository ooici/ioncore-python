#!/usr/bin/env python

"""
@file ion/play/rdf_store/association_service.py
@author David Stuebe
@brief  RDF Store: association service
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

class AssociationService(BaseService):
    """Example service implementation
    """

    # Declaration of service
    declare = BaseService.service_declare(name='associations', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        self.store = Store()
        yield self.store.create_store()

    @defer.inlineCallbacks
    def op_put_association(self, content, headers, msg):
        '''
        content is a DataObject encoding - a Dictionary!
        For storing blobs, we really just want to store it encoded.
        Can we get the serialized value from the messaging layer?
        '''
        logging.info('op_put_association: '+str(content))
        association = ValueObject(content)

        yield self.store.put(association.identity, association.value)

        # @ TODO add References!

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', {'Stored Key':association.identity}, {})

    @defer.inlineCallbacks
    def op_get_association(self, content, headers, msg):
        '''
        '''
        association = yield self.store.get(content['key'])

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', association, {})

    @defer.inlineCallbacks
    def op_del_association(self, content, headers, msg):
        '''

        '''
        # @ TODO remove References!

        yield self.store.delete(content['key'])

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', {'result':'success'}, {})


class AssociationServiceClient(BaseServiceClient):
    """
    This is an exemplar service class that calls the hello service. It
    applies the RPC pattern.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "associations"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def put_association(self, association):
        '''
        @param blob A DataObject blob to be stored
        '''
        yield self._check_init()

        assert isinstance(association, DataObject)

        (content, headers, msg) = yield self.rpc_send('put_association', association.encode())
        logging.info('Association Servie Client: put_association: '+str(content))
        defer.returnValue(content['Stored Key'])

    @defer.inlineCallbacks
    def get_association(self, key):
        '''
        @param key, A key for a stored association
        '''
        yield self._check_init()

        # assert ?
        kd={'key':key}
        (content, headers, msg) = yield self.rpc_send('get_association', kd)
        logging.info('Association Servie Client: get_association: '+str(content))
        association=DataObject.from_encoding(content)
        defer.returnValue(association)


    @defer.inlineCallbacks
    def del_association(self, key):
        '''
        @param key, A key for a stored association
        '''
        yield self._check_init()

        # assert ?
        kd={'key':key}
        (content, headers, msg) = yield self.rpc_send('del_association', kd)
        logging.info('Association Servie Client: del_association: '+str(content))
        defer.returnValue(content['result'])

# Spawn of the process using the module name
factory = ProtocolFactory(AssociationService)

