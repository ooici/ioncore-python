#!/usr/bin/env python

"""
@file ion/data/backends/store_service.py
@author Michael Meisinger
@author David Stuebe
@brief service for storing and retrieving key/value pairs.
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.core import ioninit
from ion.core.base_process import ProtocolFactory
from ion.data.store import Store, IStore
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu


CONF = ioninit.config(__name__)

class StoreService(BaseService):
    """
    Service to store and retrieve key/value pairs.
    """
    # Declaration of service
    declare = BaseService.service_declare(name='store',
                                          version='0.1.0',
                                          dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        # use spawn args to determine backend class, second config file
        backendcls = self.spawn_args.get('backend_class', CONF.getValue('backend_class', None))
        backendargs = self.spawn_args.get('backend_args', CONF.getValue('backend_args', {}))
        if backendcls:
            self.backend = pu.get_class(backendcls)
        else:
            self.backend = Store
        assert issubclass(self.backend, IStore)

        # Provide rest of the spawnArgs to init the store
        self.store = yield self.backend.create_store(**backendargs)
        logging.info("StoreService initialized")

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
        yield self.reply_ok(msg, {'result':res})

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        Service operation: Gets a value from the store identified by key.
        """
        logging.info("op_get: "+str(content))
        key = str(content['key'])
        val = yield self.store.get(key)
        yield self.reply_ok(msg, {'value':val})

    @defer.inlineCallbacks
    def op_query(self, content, headers, msg):
        """
        Service operation: Look for multiple values based on a regex on key
        """
        regex = str(content['regex'])
        res = yield self.store.query(regex)
        yield self.reply_ok(msg, {'result':res})

    @defer.inlineCallbacks
    def op_remove(self, content, headers, msg):
        """
        Service operation: Delete a value.
        """
        key = str(content['key'])
        res = yield self.store.remove(key)
        yield self.reply_ok(msg, {'result':res})

class StoreServiceClient(BaseServiceClient, IStore):
    """
    Class for the client accessing the attribute store via Exchange
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "store"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def get(self, key):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get', {'key':str(key)})
        logging.info('Service get reply: '+str(content))
        defer.returnValue(content['value'])

    @defer.inlineCallbacks
    def put(self, key, value):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('put', {'key':str(key), 'value':value})
        logging.info('Service put reply: '+str(content))
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def query(self, regex):
        (content, headers, msg) = yield self.rpc_send('query', {'regex':regex})
        logging.info('Service query reply: '+str(content))
        defer.returnValue(content['result'])

    @defer.inlineCallbacks
    def remove(self, key):
        (content, headers, msg) = yield self.rpc_send('remove', {'key':str(key)})
        logging.info('Service remove reply: '+str(content))
        defer.returnValue(content['result'])





# Spawn of the process using the module name
factory = ProtocolFactory(StoreService)
