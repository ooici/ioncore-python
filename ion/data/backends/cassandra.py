#!/usr/bin/env python
"""
@file ion/data/backends/cassandra.py
@author Paul Hubbard
@author Michael Meisinger
@author Paul Hubbard
@author Dorian Raymer
@brief Implementation of ion.data.store.IStore using pycassa to interface a
        Cassandra datastore backend
@Note Test cases for the cassandra backend are now in ion.data.test.test_store
"""

import re
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
import pycassa

from ion.core import ioninit
from ion.data.store import IStore

import uuid

CONF = ioninit.config(__name__)
CF_default_keyspace = CONF['default_keyspace']
CF_default_colfamily = CONF['default_colfamily']
CF_default_cf_super = CONF['default_cf_super']
CF_default_namespace = CONF['default_namespace']
CF_default_key = CONF['default_key']


class CassandraStore(IStore):
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    @Note Default behavior is to use a random super column name space!
    """
    def __init__(self, **kwargs):
        self.kvs = None
        self.cass_host_list = None
        self.keyspace = None
        self.colfamily = None
        self.cf_super = True
        self.namespace = None
        self.key=None

    @classmethod
    def create_store(cls, **kwargs):
        """
        @brief Factory method to create an instance of the cassandra store.
        @param kwargs keyword arguments to configure the store.
        @param cass_host_list List of hostname:ports for cassandra host or cluster
        @retval Deferred, for IStore instance.
        """
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

        if not inst.cass_host_list:
            log.info('Connecting to Cassandra on localhost...')
        else:
            log.info('Connecting to Cassandra ks:cf=%s:%s at %s ...' %
                         (inst.keyspace, inst.colfamily, inst.cass_host_list))
        inst.client = pycassa.connect(inst.cass_host_list)
        inst.kvs = pycassa.ColumnFamily(inst.client, inst.keyspace,
                                        inst.colfamily, super=inst.cf_super)
        log.info('connected to Cassandra... OK.')
        log.info('cass_host_list: '+str(inst.cass_host_list))
        log.info('keyspace: '+str(inst.keyspace))
        log.info('colfamily: '+str(inst.colfamily))
        log.info('cf_super: '+str(inst.cf_super))
        log.info('namespace: '+str(inst.namespace))
        return defer.succeed(inst)


    def clear_store(self):
        """
        @brief Delete the super column namespace. Do not touch default namespace!
        @note This is complicated by the persistence across many 
        """
        if self.cf_super:
            self.kvs.remove(self.key,super_column=self.namespace)
        else:
            log.info('Can not clear root of persistent store!')
        return defer.succeed(None)
        

    def get(self, col):
        """
        @brief Return a value corresponding to a given key
        @param col Cassandra column
        @retval Deferred, for value from the ion dictionary, or None
        """
        value = None
        try:
            if self.cf_super:
                value = self.kvs.get(self.key, columns=[col], super_column=self.namespace)
            else:
                value = self.kvs.get(self.key, columns=[col])
            #log.debug('Key "%s":"%s"' % (key, val))
            #this could fail if insert did it wrong
            value=value.get(col)
        except pycassa.NotFoundException:
            #log.debug('Key "%s" not found' % key)
            pass
        return defer.succeed(value)

    def put(self, col, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value
        @note Value is composed into OOI dictionary under keyname 'value'
        @retval Deferred for success
        """
        #log.debug('writing key %s value %s' % (key, value))
        if self.cf_super:
            self.kvs.insert(self.key, {self.namespace:{col:value}})
        else:
            self.kvs.insert(self.key, {col:value})
        return defer.succeed(None)

    def query(self, regex):
        """
        @brief Search by regular expression
        @param regex Regular expression to match against the keys
        @retval Deferred, for list, possibly empty, of keys that match.
        @note Uses get_range generator of unknown efficiency.
        """
        #@todo This implementation is very inefficient. Do smarter, but how?
        matched_list = []
        if self.cf_super:
            klist = self.kvs.get(self.key, super_column=self.namespace)
        else:
            klist = self.kvs.get(self.key)

        for x in klist.keys():
            #m = re.search(regex, x[0])
            m = re.findall(regex, x)
            if m:
                matched_list.extend(m)

        return defer.succeed(matched_list)

    def remove(self, col):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval Deferred, for success of operation
        @note Deletes are lazy, so key may still be visible for some time.
        """
        if self.cf_super:
            self.kvs.remove(self.key, columns=[col], super_column=self.namespace)
        else:
            self.kvs.remove(self.key, columns=[col])
        return defer.succeed(None)
