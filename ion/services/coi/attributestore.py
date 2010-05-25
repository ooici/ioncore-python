#!/usr/bin/env python

"""
@file ion/services/coi/attributestore.py
@author Michael Meisinger
@brief service for storing and retrieving key/value pairs.
"""

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core.base_process import ProtocolFactory
from ion.data.store import Store, IStore
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

CONF = ioninit.config(__name__)

class AttributeStoreService(BaseService):
    """
    Service to store and retrieve key/value pairs.
    """
    # Declaration of service
    declare = BaseService.service_declare(name='attributestore',
                                          version='0.1.0',
                                          dependencies=[])

    def slc_init(self):
        # use spawn args to determine backend class, second config file
        backendcls = self.spawnArgs.get('backend_class', CONF.getValue('backend_class', None))
        if backendcls:
            self.backend = pu.get_class(backendcls)
        else:
            self.backend = Store
        assert issubclass(self.backend, IStore)
        self.store = self.backend()
        # Provide rest of the spawnArgs to init the store
        self.store.init(**self.spawnArgs)
        logging.info("AttributeStoreService initialized")

    @defer.inlineCallbacks
    def op_put(self, content, headers, msg):
        """
        Service operation: Puts a value into the store identified by key.
        Replies with a result of this operation
        """
        logging.info("op_put: "+str(content))
        key = str(content['key'])
        val = content['value']
        res = yield self.store.put(key, val)
        yield self.reply(msg, 'result', {'status':'OK', 'result':res})

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        Service operation: Gets a value from the store identified by key.
        """
        logging.info("op_get: "+str(content))
        key = str(content['key'])
        val = yield self.store.get(key)
        yield self.reply(msg, 'result', {'status':'OK', 'value':val})

    @defer.inlineCallbacks
    def op_query(self, content, headers, msg):
        """
        Service operation: Look for multiple values based on a regex on key
        """
        # @todo implement

    @defer.inlineCallbacks
    def op_delete(self, content, headers, msg):
        """
        Service operation: Delete a value.
        """
        # @todo implement


class AttributeStoreClient(BaseServiceClient):
    """
    Class for the client accessing the attribute store via Exchange
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "attributestore"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def put(self, key, value):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('put', {'key':str(key), 'value':value})
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def get(self, key):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get', {'key':str(key)})
        logging.info('Service reply: '+str(content))
        defer.returnValue(content['value'])

# Spawn of the process using the module name
factory = ProtocolFactory(AttributeStoreService)
