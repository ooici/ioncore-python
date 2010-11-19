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

from ion.core import ioninit
from ion.data import store 

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



class CassandraStore(object):
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

    namespace = 'default' # implemented as cassandra column family
                          # (Telephus columnPath)

    def __init__(self, client):
        """functional wrapper around active client instance
        """
        self.client = client

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
            log.debug("Didn't find the key: %s. Returning None" % col)     
            value = None
        defer.returnValue(value)

    @defer.inlineCallbacks
    def put(self, col, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value
        @note Value is composed into OOI dictionary under keyname 'value'
        @retval Deferred for success
        """
        log.info("CassandraStore: Calling put on col: %s  value: %s " % (col, value))
        try:
            if self.cf_super:
                log.info("CassandraStore: super_col key %s colfamily %s value %s column %s super_column %s " % (self.key, self.colfamily, value, col, self.namespace))
                yield self.client.insert(self.key, self.colfamily, value, column=col, super_column=self.namespace) 
            else:
                yield self.client.insert(self.key, self.colfamily, value, column=col)
        except:
            log.info("CassandraStore: Exception was thrown during the put")
        defer.returnValue(None)

    @defer.inlineCallbacks
    def query(self, regex):
        """
        @brief Search by regular expression
        @param regex Regular expression to match against the keys
        @retval Deferred, for list, possibly empty, of keys that match.
        @note Uses get_range generator of unknown efficiency.
        """
        log.info("searching for regex %s" % regex)
        matched_list = []
        if self.cf_super:
            klist = yield self.client.get(self.key, self.colfamily, super_column=self.namespace)
        else:
            klist = yield self.client.get_slice(self.key, self.colfamily)
        
        #This code could probably be refactored. The data structures returned are different if
        #it is called with a column or super_column. Another possibility is that the code 
        #is removed when the IStore interface doesn't use the query interface.
        if self.cf_super:
            columns = klist.super_column.columns
            for col in columns:
            
                m = re.findall(regex, str(col.name))
                
                if m: 
                    matched_list.extend(m)
        else:
            for col in klist:
                m = re.findall(regex, str(col.column.name))
                if m:
                    matched_list.extend(m)

        log.info("matched_list %s" % matched_list)
        defer.returnValue(matched_list)

    @defer.inlineCallbacks
    def remove(self, col):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval Deferred, for success of operation
        @note Deletes are lazy, so key may still be visible for some time.
        """
        if self.cf_super:
            yield self.client.remove(self.key, self.colfamily, column=col, super_column=self.namespace)
        else:
            yield self.client.remove(self.key, self.colfamily, column=col)
        defer.returnValue(None)


class CassandraFactory(object):
    """
    The store class attribute is the IStore adapter class that will be used
    to Adapt the cassandra client instance.

    @note Design note: This is more of an Adapter than a pure Factory. The
    intended use is not necessarily to create an arbitrary number of
    cassandra client instances, but really to automate the creation of one
    client, and then adapt that client to conform to the IStore interface.
    """
    
    # This is the Adapter class. The default, CassandraStore, implements
    # the IStore interface. You can assign other Adapters here, if you want
    # something besides IStore.
    store = CassandraStore

    cassandraKeyspace = "Keyspace1"

    def __init__(self, host='127.1.0.1', port=9160, process=None):
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
        self.host = host
        self.port = port
        if process is None:
            process = reactor
        self.process = process

    def buildStore(self, namespace):
        """
        @param namespace Maps to Cassandra specific columnFamily option
        @note For cassandra, there needs to be a conventionaly used
        Keyspace option.
        """
        # @note The cassandra KeySpace is used to implement the IStore namespace
        # concept.
        f = ManagedCassandraClientFactory()
        client = CassandraClient(f, keyspace=self.cassandraKeyspace) 
        self.process.connectTCP(self.host, self.port, f)
        # What we have with this
        # CassandraFactory class is a mixture of a Factory pattern and an
        # Adapter pattern. s is our IStore providing instance the user of
        # the factory expects. If we were to make a general "StoreFactory"
        # or maybe even an "IStoreFactory" interface, it's behavior would
        # be to build/carryout the mechanics of a TCP client connection AND
        # then Adapting it and returning the result as an IStore providing
        # instance.
        s = self.store(client)
        s.namespace = namespace
        return s


