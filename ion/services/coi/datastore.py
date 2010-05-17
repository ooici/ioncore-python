#!/usr/bin/env python

"""
@file ion/services/coi/datastore.py
@author Michael Meisinger
@brief service for storing and retrieving stateful data objects.
"""

import logging
from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.data.objstore import ObjectStore
from ion.services.base_service import BaseService, BaseServiceClient


class DatastoreService(BaseService):
    """
    Service to store and retrieve structured objects. Updating an object
    will modify the object's state but keep the state history in place. It is
    always possible to get value objects
    """
    # Declaration of service
    declare = BaseService.service_declare(name='datastore', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.os = ObjectStore()
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
        val = content['value']
        parents = content['parents'] if 'parents' in content else None
        commitref = yield self.os.put(key, val, parents, committer=headers['sender'])
        yield self.reply(msg, 'result', commitref.identity)

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """Service operation: Gets a structured object from the data store.
        Equivalent to a git-pull.
        """
        logging.info("op_get: "+str(content))
        key = content['key']
        val = yield self.os.get(key)
        yield self.reply(msg, 'result', val.encode(), {})

    @defer.inlineCallbacks
    def op_get_values(self, content, headers, msg):
        """Service operation: Gets values from the object store.
        """
        logging.info("op_get_values: "+str(content))
        keys = content['keys']
        vals = yield self.os.getmult(keys)
        resvals = []
        for val in vals:
            resvals.append(val.encode())
        yield self.reply(msg, 'result', resvals)

    @defer.inlineCallbacks
    def op_get_ancestors(self, content, headers, msg):
        """Service operation: Gets all ancestors of a value.
        """
        logging.info("op_get_ancestors: "+str(content))
        key = str(content['key'])
        
        resvalues = []
        cref = yield self.os.get_commitref(key)
        ancs = yield self.os.vs.get_ancestors(cref)
        yield self.reply(msg, 'result', ancs)


class DatastoreClient(BaseServiceClient):
    """
    Class for the client accessing the object store service via ION Exchange
    """
    def __init__(self, proc=None, pid=None):
        BaseServiceClient.__init__(self, "datastore", proc, pid)

    @defer.inlineCallbacks
    def put(self, key, value, parents=None):
        yield self._check_init()
        cont = {'key':str(key), 'value':value}
        if parents and parents is list:
            cont['parents'] = basedon
        elif parents:
            cont['parents'] = [parents]
        (content, headers, msg) = yield self.proc.rpc_send(self.svc, 'put', cont)
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def get(self, key):
        yield self._check_init()
        (content, headers, msg) = yield self.proc.rpc_send(self.svc, 'get', {'key':str(key)})
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content['value']))

class DatastoreDirectClient(BaseServiceClient):
    """
    Class for the client accessing the an object store via an out-of-band
    backend technology, such as a Cassandra or Redis client.
    """


class DatastoreServiceClient(BaseServiceClient):
    pass

# Spawn of the process using the module name
factory = ProtocolFactory(DatastoreService)

"""
from ion.services.coi import datastore as d
spawn(d)
send (1, {'op':'put','content':{'key':'k1','value':'v'}})
"""
