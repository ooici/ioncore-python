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
from ion.core.process import process

from ion.core.managedconnections import tcp


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

class CassandraError(Exception):
    """
    An exception class for ION Cassandra Client errors
    """

class CassandraStore(tcp.TCPConnection):
    """
    An Adapter class that implements the IStore interface by way of a
    cassandra client connection. As an adapter, this assumes an active
    client (it implements/provides no means of connection management).
    The same client instance could be used by another adapter class that
    implements another interface.
    
    @note: This is how we map the OOI architecture terms to Cassandra.  
    persistent_technology --> hostname, port
    persistent_archive --> keyspace
    cache --> columnfamily

    @todo Provide explanation of the cassandra options 
     - keyspace: Outermost context within a Cassandra server (like vhost).
     - column family: Like a database table. 
    """

    implements(store.IStore)

    def __init__(self, persistent_technology, persistent_archive, credentials, cache):
        """
        functional wrapper around active client instance
        """
        
        ### Get the host and port from the Persistent Technology resource
        host = persistent_technology.hosts[0].host
        port = persistent_technology.hosts[0].port
        
        ### Get the Key Space for the connection
        self._keyspace = persistent_archive.name
        # More robust but obfuscates errors in calling arguments
        #self._keyspace = getattr(persistent_archive, 'name', None)
        
        #Get the credentials for the cassandra connection
        uname = credentials.username
        pword = credentials.password
        authorization_dictionary = {'username': uname, 'password': pword}
        
        ### Create the twisted factory for the TCP connection  
        self._manager = ManagedCassandraClientFactory(keyspace=self._keyspace, credentials=authorization_dictionary)
        #self._manager = ManagedCassandraClientFactory(keyspace=self._keyspace, credentials=None)
        
        # Call the initialization of the Managed TCP connection base class
        tcp.TCPConnection.__init__(self,host,port,self._manager)
        self.client = CassandraClient(self._manager)    
        
        
        ### Get the column family name from the Cache resource
        #if hasattr(cache, 'name'):
        #    raise CassandraError, 'Cassandra Store must be initalized with a ION Cache Resource using the cfCache keyword argument'
        self._cfCache = cache.name # Cassandra Column Family maps to an ION Cache resource
        

    @defer.inlineCallbacks
    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key 
        @retval Deferred that fires with the value of key
        """
        
        log.debug("CassandraStore: Calling get on col %s " % key)
        try:
            result = yield self.client.get(key, self._cfCache, column='value')
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
        yield self.client.insert(key, self._cfCache, value, column='value')

    @defer.inlineCallbacks
    def remove(self, key):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval Deferred, for success of operation
        @note Deletes are lazy, so key may still be visible for some time.
        """
        yield self.client.remove(key, self._cfCache, column='value')
    
    def on_deactivate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_deactivate: Lose Connection TCP')

    def on_terminate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_terminate: Lose Connection TCP')

class CassandraStorageResource:
    """
    This class holds the connection information in the
    persistent_technology, persistent_archive, cache, and 
    credentials
    """
    def __init__(self, persistent_technology, persistent_archive=None, cache=None, credentials=None):
        self.persistent_technology = persistent_technology
        self.persistent_archive = persistent_archive
        self.cache = cache
        self.credentials = credentials
        
    def get_host(self):
        return self.persistent_technology.hosts[0].host
    
    def get_port(self):
        return self.persistent_technology.hosts[0].port
    
    def get_credentials(self):    
        uname = self.credentials.username
        pword = self.credentials.password
        authorization_dictionary = {'username': uname, 'password': pword}
        return authorization_dictionary
    
class CassandraDataManager(tcp.TCPConnection):

    #implements(store.IDataManager)

    def __init__(self, storage_resource):
        """
        @param storage_resource provides the connection information to connect to the Cassandra cluster.
        """
        host = storage_resource.get_host()
        port = storage_resource.get_port()
        authorization_dictionary = storage_resource.get_credentials()
        
        self._manager = ManagedCassandraClientFactory(credentials=authorization_dictionary)
        
        tcp.TCPConnection.__init__(self,host,port,self._manager)
        self.client = CassandraClient(self._manager)    
        
        
    @defer.inlineCallbacks
    def create_persistent_archive(self, persistent_archive):
        """
        @brief Create a Cassandra Keyspace
        @param persistent_archive is an ion resource which defines the properties of a Key Space
        @retval ?
        """
        keyspace = persistent_archive.name
        #Check to see if replication_factor and strategy_class is defined in the persistent_archive
        ksdef = KsDef(name=keyspace, replication_factor=1,
                strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                cf_defs=[])
        yield self.client.system_add_keyspace(ksdef)

    def update_persistent_archive(self, pa):
        """
        @brief Update a Cassandra Keyspace
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

    
    def on_deactivate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_deactivate: Loose Connection TCP')

    def on_terminate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_terminate: Loose Connection TCP')


### Currently not used...
#class CassandraFactory(process.ProcessClientBase):
#    """
#    The store class attribute is the IStore adapter class that will be used
#    to Adapt the cassandra client instance.
#
#    @note Design note: This is more of an Adapter than a pure Factory. The
#    intended use is not necessarily to create an arbitrary number of
#    cassandra client instances, but really to automate the creation of one
#    client, and then adapt that client to conform to the IStore interface.
#    """
#    
#    # This is the Adapter class. It generates client connections for any
#    # cassandra class which is instantiated by init(client, kwargs)
#
#    def __init__(self, proc=None, presistence_technology=None):
#        """
#        @param host defaults to localhost
#        @param port 9160 is the cassandra default
#        @param process instance of ion process. If you are calling from an
#        ion Service, then pass in 'self'. If you need to, you can pass in
#        the reactor object.
#        @note This is an experimental idea
#        @todo Decide on good default for namespace
#        @note Design Note: These are standard parameters that any StoreFactory 
#        would need. In particular, the namespace parameter is an
#        implementation choice to fulfill a [not fully articulated]
#        architectural need.
#        """
#        ProcessClientBase.__init__(self, proc=process, **kwargs)
#        
#        self.presistence_technology = presistence_technology
#        
#
#    @defer.inlineCallbacks
#    def buildStore(self, IonCassandraClient, **kwargs):
#        """
#        @param namespace Maps to Cassandra specific columnFamily option
#        @note For cassandra, there needs to be a conventionaly used
#        Keyspace option.
#        """
#        
#        # What we have with this
#        # CassandraFactory class is a mixture of a Factory pattern and an
#        # Adapter pattern. s is our IStore providing instance the user of
#        # the factory expects. If we were to make a general "StoreFactory"
#        # or maybe even an "IStoreFactory" interface, it's behavior would
#        # be to build/carryout the mechanics of a TCP client connection AND
#        # then Adapting it and returning the result as an IStore providing
#        # instance.
#        
#        # Create and instance of the ION Client class
#        instance = IonCassandraClient(self.presistence_technology, **kwargs)
#        
#        yield defer.maybeDeferred(self.proc.register_life_cycle_objects.append, instance)
#        
#        defer.returnValue(instance)

