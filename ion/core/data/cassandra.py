#!/usr/bin/env python
"""
@file ion/data/backends/cassandra.py
@author Paul Hubbard
@author Michael Meisinger
@author Paul Hubbard
@author Dorian Raymer
@author Matt Rodriguez
@brief Implementation of ion.data.store.IStore using Telephus to interface a
        Cassandra datastore backend
@note Test cases for the cassandra backend are now in ion.data.test.test_store
"""

import re
import uuid

from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import components

from zope.interface import implements

from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory
from telephus.cassandra.ttypes import NotFoundException
from telephus.cassandra.ttypes import KsDef
from telephus.cassandra.ttypes import CfDef

from ion.core import ioninit
from ion.core.data import store 

from ion.util.state_object import BasicLifecycleObject

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# Moving to not use CONF. In the store service module, spawn args can be
# used to pass appropriate configuration parameters.
#CONF = ioninit.config(__name__)
#CF_default_keyspace = CONF['default_keyspace']
#CF_default_colfamily = CONF['default_colfamily']
#CF_default_cf_super = CONF['default_cf_super']
#CF_default_namespace = CONF['default_namespace']
#CF_default_key = CONF['default_key']



class CassandraStore(BasicLifecycleObject):
    """
    An Adapter class that implements the IStore interface by way of a
    cassandra client connection. As an adapter, this assumes an active
    client (it implements/provides no means of connection management).
    The same client instance could be used by another adapter class that
    implements another interface.

    @todo Provide explanation of the cassandra options 
     - keyspace: Outermost context within a Cassandra server (like vhost).
     - column family: Like a database table. 
    """

    implements(store.IStore)

    def __init__(self, client, namespace=None):
        """functional wrapper around active client instance
        """
        self.client = client
        
        self.namespace = namespace # Cassandra Column Family!

    @defer.inlineCallbacks
    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key 
        @retval Deferred that fires with the value of key
        """
        
        log.debug("CassandraStore: Calling get on col %s " % key)
        try:
            result = yield self.client.get(key, self.namespace, column='value')
            value = result.column.value
        except NotFoundException:
            log.debug("Didn't find the key: %s. Returning None" % key)     
            value = None
        defer.returnValue(value)

    @defer.inlineCallbacks
    def put(self, key, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value
        @note Value is composed into OOI dictionary under keyname 'value'
        @retval Deferred for success
        """
        log.debug("CassandraStore: Calling put on key: %s  value: %s " % (key, value))
        # @todo what exceptions need to be handled for an insert?
        yield self.client.insert(key, self.namespace, value, column='value')

    @defer.inlineCallbacks
    def remove(self, key):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval Deferred, for success of operation
        @note Deletes are lazy, so key may still be visible for some time.
        """
        yield self.client.remove(key, self.namespace, column='value')

    def on_initialize(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_activate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_deactivate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_terminate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_error(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

class CassandraManager(BasicLifecycleObject):

    #implements(store.IDataManager)

    def __init__(self, client):
        self.client = client
        self.storage_resource

    def create_persistent_archive(self, pa):
        """
        @brief Create a Cassandra Key Space
        @param pa is a persistent archive object which defines the properties of a Key Space
        @retval ?
        """
        #ksdef = KsDef(name=keyspace, replication_factor=1,
        #        strategy_class='org.apache.cassandra.locator.SimpleStrategy',
        #        cf_defs=[])
        #return self.client.system_add_keyspace(ksdef)

    def update_persistent_archive(self, pa):
        """
        @brief Update a Cassandra Key Space
        This method should update the Key Space properties - not change the column families!
        @param pa is a persistent archive object which defines the properties of a Key Space
        @retval ?
        """
        
    
    def remove_persistent_archive(self, pa):
        """
        @brief Remove a Cassandra Key Space
        @param pa is a persistent archive object which defines the properties of a Key Space
        @retval ?
        """
        

    def create_cache(self, pa, cache):
        """
        @brief Create a Cassandra column family
        @param pa is a persistent archive object which defines the properties of an existing Key Space
        @param cache is a cache object which defines the properties of column family
        @retval ?
        """
        #cfdef = CfDef(keyspace=self.keyspace, name=name)
        #return self.client.system_add_column_family(cfdef)

    def remove_cache(self, pa, cache):
        """
        @brief Remove a Cassandra column family
        @param pa is a persistent archive object which defines the properties of an existing Key Space
        @param cache is a cache object which defines the properties of column family
        @retval ?
        """
        #return self.system_drop_column_family(name)

    def update_cache(self, pa, cache):
        """
        @brief Update a Cassandra column family
        @param pa is a persistent archive object which defines the properties of an existing Key Space
        @param cache is a cache object which defines the properties of column family
        @retval ?
        """

    def on_initialize(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_activate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_deactivate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_terminate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    def on_error(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

class CassandraFactory(process.ProcessClientBase):
    """
    The store class attribute is the IStore adapter class that will be used
    to Adapt the cassandra client instance.

    @note Design note: This is more of an Adapter than a pure Factory. The
    intended use is not necessarily to create an arbitrary number of
    cassandra client instances, but really to automate the creation of one
    client, and then adapt that client to conform to the IStore interface.
    """
    
    # This is the Adapter class. It generates client connections for any
    # cassandra class which is instantiated by init(client, kwargs)

    def __init__(self, proc=None, storage_deployment=None):
        """
        @param host defaults to localhost
        @param port 9160 is the cassandra default
        @param process instance of ion process. If you are calling from an
        ion Service, then pass in 'self'. If you need to, you can pass in
        the reactor object.
        @note This is an experimental idea
        @todo Decide on good default for namespace
        @note Design Note: These are standard parameters that any StoreFactory 
        would need. In particular, the namespace parameter is an
        implementation choice to fulfill a [not fully articulated]
        architectural need.
        """
        ProcessClientBase.__init__(self, proc=process, **kwargs)
        
        self.storage_deployment = storage_deployment
        #if process is None:
        #    process = reactor
        #self.process = process

    def buildStore(self, credential, clazz, **kwargs):
        """
        @param namespace Maps to Cassandra specific columnFamily option
        @note For cassandra, there needs to be a conventionaly used
        Keyspace option.
        """
        
        # Get the keyspace if any
        keyspace = kwargs.get('keyspace',None)
        
        # Get the 
        host = self.storage_deployment.hosts[0].host
        port = self.storage_deployment.hosts[0].port
        
        #@TODO pass the credentials
        manager = ManagedCassandraClientFactory(**kwargs)

        client = CassandraClient(manager)        
        self.proc.connectTCP(self.host, self.port, manager)
        
        # What we have with this
        # CassandraFactory class is a mixture of a Factory pattern and an
        # Adapter pattern. s is our IStore providing instance the user of
        # the factory expects. If we were to make a general "StoreFactory"
        # or maybe even an "IStoreFactory" interface, it's behavior would
        # be to build/carryout the mechanics of a TCP client connection AND
        # then Adapting it and returning the result as an IStore providing
        # instance.
        
        
        
        
        instance = clazz(client, **kwargs)
        
        self.proc.registerd_life_cycle_objects.append(instance)
        
        
        return instance


class CassandraMangerFactory(CassandraFactory):
    
    store = CassandraManager
