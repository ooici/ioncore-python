#!/usr/bin/env python

"""
@file ion/play/rdf_store/association_service.py
@author David Stuebe
@brief  RDF Store: association service
"""

import logging
from twisted.internet import defer

from ion.data.store import Store, IStore
from ion.play.rdf_store.rdf_base import RdfAssociation

class AssociationStore(object):
    """Example service implementation
    """

    def __init__(self, backend=None, backargs=None):
        """
        @param backend  Class object with a compliant Store or None for memory
        @param backargs arbitrary keyword arguments, for the backend
        """
        self.backend = backend if backend else Store
        self.backargs = backargs if backargs else {}
        assert issubclass(self.backend, IStore)
        assert type(self.backargs) is dict

        # KVS with value ID -> value
        self.store = None

    #@TODO make this also a class method so it is easier to start - one call?
    @defer.inlineCallbacks
    def init(self):
        """
        Initializes the ValueStore class
        @retval Deferred
        """
        self.store = yield self.backend.create_store(**self.backargs)
        logging.info("AssociationStore initialized")


    @defer.inlineCallbacks
    def put_association(self, association):
        '''
        '''
        assert isinstance(association, RdfAssociation)
        yield self.store.put(association)

        # @ TODO add References!

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

