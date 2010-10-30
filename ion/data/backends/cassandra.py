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

CONF = ioninit.config(__name__)
CF_default_keyspace = CONF['default_keyspace']
CF_default_colfamily = CONF['default_colfamily']
CF_default_cf_super = CONF['default_cf_super']
CF_default_namespace = CONF['default_namespace']
CF_default_key = CONF['default_key']


class CassandraFactory(object):
    """
    """


    def __init__(self, keyspace, host='127.1.0.1', port=9160, process=None):
        """
        @param keyspace Cassandra specific option (use 'ion' ?)
        @param host defaults to localhost
        @param port 9160 is the cassandra default
        """
        self.host = host
        self.port = port
        self.keyspace = None

    def buildStore(self, keyspace):
        """
        The keyspace could be an arg here.
        """
        f = ManagedCassandraClientFactory()
        client = CassandraClient(f, self.keyspace) 
        connector = reactor.connectTCP(self.host, self.port, f)

class CassandraStore(object):
    """
    Store interface for interacting with the Cassandra key/value store
    A Cassandra client connects to a particular Keyspace within a Cassandra
    server at a particular Host/Port.
    @note Default behavior is to use a random super column name space!
    @todo Provide explanation of the cassandra options 
     - keyspace
       Outer-most level of organization. Usually the name of the
       application (e.g. Twitter, Ion). 
     - column family
       A column family is like a database table. The Store namespace maps
       to the name of a column family (column path).
    """

    implements(store.IStore)

    def __init__(self, client):
        """functional wrapper around active client instance
        """
        self.client = client


    @classmethod
    def create_store(cls, **kwargs):
        """
        @brief Factory method to create an instance of the cassandra store.
        @param kwargs keyword arguments to configure the store.
        @param cass_host_list List of hostname:ports for cassandra host or cluster
        @retval Deferred, for IStore instance.
        """
        log.info('In create_store method')
        inst = cls(**kwargs)
        inst.kwargs = kwargs
        inst.cass_host_list = kwargs.get('cass_host_list', None)
        inst.keyspace = kwargs.get('keyspace', CF_default_keyspace)
        inst.colfamily = kwargs.get('colfamily', CF_default_colfamily)
        inst.cf_super = kwargs.get('cf_super', CF_default_cf_super)
        inst.key = kwargs.get('key', CF_default_key)
        
        if not inst.key:
            inst.key = str(uuid.uuid4())
        
        if inst.cf_super:
            inst.namespace = kwargs.get('namespace', CF_default_namespace)
            if inst.namespace == None:
                # Must change behavior to set a random namespace so that test don't interfere!
                inst.namespace = ':'
        else:
            if inst.namespace:
                log.info('Ignoring namespace argument in non super column cassandra store')
            inst.namespace=None
        
        if  inst.cass_host_list is None:
            port = 9160
            host = 'amoeba.ucsd.edu'
        else:
            port = int(inst.cass_host_list[0].split(":")[1])
            host = inst.cass_host_list[0].split(":")[0]
            log.info("Got host %s and port %d from cass_host_list" % (host, port))
                
        inst.manager = ManagedCassandraClientFactory()
        inst.client = CassandraClient(inst.manager, inst.keyspace) 
        inst.connector = reactor.connectTCP(host, port, inst.manager, timeout=1)
        log.info("Created Cassandra store")
        return defer.succeed(inst)               

    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key 
        @retval Deferred that fires with the value of key
        """
        
        log.info("CassandraStore: Calling get on col %s " % key)
        try:
            value = yield self.client.get(key, self.colfamily, column='value')
        except NotFoundException:
            log.info("Didn't find the col: %s. Returning None" % col)     
            defer.returnValue(None)
            
        column_value = value.column.value 
        defer.returnValue(column_value)

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
