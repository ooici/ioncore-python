#!/usr/bin/env python

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core.base_process import ProtocolFactory
from ion.data.objstore import ObjectStore
from ion.data.store import Store, IStore
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

from ion.data.dataobject import DataObject

CONF = ioninit.config(__name__)

class StateStoreService(BaseService):
    """
    Service to store and retrieve structured objects. Updating an object
    will modify the object's state but keep the state history in place. It is
    always possible to get value objects
    """
    # Declaration of service
    declare = BaseService.service_declare(name='StateStore', version='0.1.0', dependencies=[])

    def slc_init(self):
        # use spawn args to determine backend class, second config file
        #backendcls = self.spawnArgs.get('backend_class', CONF.getValue('backend_class', None))
        #if backendcls:
        #    self.backend = pu.get_class(backendcls)
        #else:
        #    self.backend = Store
        #assert issubclass(self.backend, IStore)
        #
        #self.os = ObjectStore(backend=self.backend)
        self.os = ObjectStore(backend=Store)
        logging.info("DatastoreService initialized")

    @defer.inlineCallbacks
    def op_put(self, content, headers, msg):
        """
        Service operation: Puts a structured object into the data store.
        Equivalent to a git-push, with an already locally commited object.
        Replies with a result with the identity of the commit value
        """
        logging.info("op_put: "+str(content))
        key = content['key']
        val = DataObject.from_encoding(content['value'])
        parents = content['parents'] if 'parents' in content else None
        commitref = yield self.os.put(key, val, parents, committer=headers['sender'])
        yield self.reply(msg, 'result', commitref.identity)

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        Service operation: Gets a structured object from the data store.
        Equivalent to a git-pull.
        """
        logging.info("op_get: "+str(content))
        key = content['key']
        
        commit=None
        if 'commit' in content:
            commit=content['commit']

        val = yield self.os.get(key,commit)
        
        if val:
            val=val.encode()
        yield self.reply(msg, 'result',val , {})

factory = ProtocolFactory(StateStoreService)

class StateServiceClient(BaseServiceClient):
    """
    Class for the client accessing the object store service via ION Exchange
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "StateStore"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def put(self, key, value, parents=None):
        yield self._check_init()
        cont = {'key':str(key), 'value':{'state':list(value)}}

        # Parents can be a string id, a list of strings, a tuple of strings or a set of strings.
        if type(parents) is set:
            cont['parents'] = list(parents)
        elif parents:
            cont['parents'] = [parents]

        (content, headers, msg) = yield self.rpc_send('put', cont)
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def get(self, key,commit=None):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get', {'key':str(key),'commit':commit})
        logging.info('Service reply: '+str(content))
        defer.returnValue(set(content['state']))
