#!/usr/bin/env python


"""
@file ion/core/data/index_store_service.py
@author Matt Rodriguez
@author David Stuebe
@brief Service which fronts the index store capability through the messaging to a single back end.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.object import object_utils

from ion.core.data.store import IStore, Store


from zope.interface import implements

from ion.core import ioninit
CONF = ioninit.config(__name__)



QUERY_ATTRIBUTES_TYPE = object_utils.create_type_identifier(object_id=17, version=1)
ROW_TYPE = object_utils.create_type_identifier(object_id=18, version=1)
ROWS_TYPE = object_utils.create_type_identifier(object_id=19, version=1)
INDEXED_ATTRIBUTES_TYPE = object_utils.create_type_identifier(object_id=20, version=1)
ROW_INDEX_UPDATE_TYPE = object_utils.create_type_identifier(object_id=21, version=1)


class StoreServiceException(Exception):
    """
    Exceptions that originate in the IndexStoreService class
    """

class StoreService(ServiceProcess):
    """
    @brief IndexStoreService

    This is not a ION service. It is part of a test harness to provide a pure, in memory backend for the data store
    and the association service

    TODO, this class does not catch any exceptions from the business logic class. 
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='store_service', version='0.1.0', dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)

        log.info(self.spawn_args)
        if self.spawn_args.get('indices'):
            # Check to make sure store and index store were not mixed up...
            raise StoreServiceException('Invalid Spawn Arg indicies passed to store service!')

            
            
    #@defer.inlineCallbacks
    def slc_activate(self, *args):
        """
        Activation can be automatic when the process is spawned or triggered by
        a message from the client.
        
        First, default to bootstrapping from the spawn args. Create all resources
        by describing the cluster!
        
        Second, add hooks to override the spawn args and take the storage resource
        reference from a message in op_activate - connecting to an already active
        system!
        
        """
        
        self._store = Store()

        log.info("Created Index Store Service")
        

            
    @defer.inlineCallbacks
    def op_put(self, request, headers, msg):
        """
        @note, puts a row into the Cassandra cluster. 
        @retval does not return anything
        """
        key = request.key
        value = request.value

        yield self._store.put(key,value)

        yield self.reply_ok(msg)


    @defer.inlineCallbacks
    def op_get(self, request, headers, msg):
        """
        @note Gets a row from the Cassandra cluster
        If the row does not exist then leave the value field in the CassandraIndexedRow empty.
        @param request is a CassandraRow message object
        @retval Returns a CassandraRow message in the response   
        """

        value = yield self._store.get(request.key)
        response = yield self.message_client.create_instance(ROW_TYPE)
        response.key = request.key
        
        if value is not None:
            response.value = value

        # Consider using raise with a not found response?
        yield self.reply_ok(msg, response)
         
    @defer.inlineCallbacks
    def op_remove(self, request, headers, msg): 
        """
        @note removes a row
        @param request is a CassandraRow message object
        @retval does not return anything
        """     

        yield self._store.remove(request.key)
        yield self.reply_ok(msg)
        
    @defer.inlineCallbacks
    def op_has_key(self, request, headers, msg):
        """
        @note sees if key exists in the cluster
        @request is a CassandraRow message object
        @retval return a string that is "True" or "False" in a CassandraRow message
        """
        key_exists = yield self._store.has_key(request.key)
        log.info("key_exists: " + str(key_exists))
        response = yield self.message_client.create_instance(ROW_TYPE)
        response.value = str(int(key_exists))
        yield self.reply_ok(msg, response)

        
# Spawn of the process using the module name
factory = ProcessFactory(StoreService)


class StoreServiceClient(ServiceClient):
    """
    This interface will change, because we have to define the ION resources. We probably want
    convenience methods to query by name, type, etc...
    
    TODO have this implement the Indexstore interface
    """
    implements(IStore)
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'store_service'
        ServiceClient.__init__(self, proc, **kwargs)
        
        self.mc = proc.message_client

        
    @defer.inlineCallbacks
    def put(self, key, value):
        log.info("Called Store Service client: put")
        
        row = yield self.mc.create_instance(ROW_TYPE)
        row.key = key
        row.value = value
        
        (content, headers, msg) = yield self.rpc_send('put', row)
        

        defer.returnValue(content)


    @defer.inlineCallbacks
    def get(self, key):
        log.info("Called Store Service client: get")
        row = yield self.mc.create_instance(ROW_TYPE)
        row.key = key
          
        (result, headers, msg) = yield self.rpc_send('get',row)

        if not result.value is '':
            defer.returnValue(result.value)
        else:
            defer.returnValue(None)
        
    @defer.inlineCallbacks
    def remove(self, key):
        log.info("Called Store Service client: remove")
        row = yield self.mc.create_instance(ROW_TYPE)
        row.key = key
          
        (content, headers, msg) = yield self.rpc_send('remove', row)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def has_key(self, key):
        log.info("Called Store Service client: has_key")
        row = yield self.mc.create_instance(ROW_TYPE)
        row.key = key
        (result, headers, msg) = yield self.rpc_send('has_key', row)
        ret = bool(int(result.value))
        log.info("%s" % (ret,))
        defer.returnValue(ret)