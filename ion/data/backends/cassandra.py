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
import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer
import pycassa

from ion.core import ioninit
from ion.data.store import IStore

CONF = ioninit.config(__name__)
CF_default_keyspace = CONF['default_keyspace']
CF_default_colfamily = CONF['default_colfamily']
CF_default_cf_super = CONF['default_cf_super']
CF_default_namespace = CONF['default_namespace']

class CassandraStore(IStore):
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    """
    def __init__(self, **kwargs):
        self.kvs = None
        self.cass_host_list = None
        self.keyspace = None
        self.colfamily = None
        self.cf_super = False
        self.namespace = None

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
        inst.namespace = kwargs.get('namespace', CF_default_namespace)
        if inst.cf_super and inst.namespace == None:
            inst.namespace = ":"

        if not inst.cass_host_list:
            logging.info('Connecting to Cassandra on localhost...')
        else:
            logging.info('Connecting to Cassandra ks:cf=%s:%s at %s ...' %
                         (inst.keyspace, inst.colfamily, inst.cass_host_list))
        inst.client = pycassa.connect(inst.cass_host_list)
        inst.kvs = pycassa.ColumnFamily(inst.client, inst.keyspace,
                                        inst.colfamily, super=inst.cf_super)
        logging.info('connected to Cassandra... OK.')
        return defer.succeed(inst)

    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key Cassandra key
        @retval Deferred, for value from the ion dictionary, or None
        """
        value = None
        try:
            if self.cf_super:
                val = self.kvs.get(key, super_column=self.namespace)
            else:
                if self.namespace:
                    key = self.namespace + ":" + key
                val = self.kvs.get(key)
            #logging.debug('Key "%s":"%s"' % (key, val))
            #this could fail if insert did it wrong
            value = val['value']
        except pycassa.NotFoundException:
            #logging.debug('Key "%s" not found' % key)
            pass
        return defer.succeed(value)

    def put(self, key, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value
        @note Value is composed into OOI dictionary under keyname 'value'
        @retval Deferred for success
        """
        #logging.debug('writing key %s value %s' % (key, value))
        if self.cf_super:
            self.kvs.insert(key, {self.namespace:{'value':value}})
        else:
            if self.namespace:
                key = self.namespace + ":" + key
            self.kvs.insert(key, {'value':value})
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
            klist = self.kvs.get_range(super_column=self.namespace)
            for x in klist:
                if re.search(regex, x[0]):
                    matched_list.append(x)
        else:
            klist = self.kvs.get_range()
            if self.namespace:
                prefix = self.namespace+":" if self.namespace else ''
                pl = len(prefix)
                for x in klist:
                    key = x[0]
                    if key.startswith(prefix) and re.search(regex, key[pl:]):
                        y = (key[pl:], x[1])
                        matched_list.append(y)
            else:
                for x in klist:
                    if re.search(regex, x[0]):
                        matched_list.append(x)

        return defer.succeed(matched_list)

    def remove(self, key):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval Deferred, for success of operation
        @note Deletes are lazy, so key may still be visible for some time.
        """
        if self.cf_super:
            self.kvs.remove(key, super_column=self.namespace)
        else:
            if self.namespace:
                key = self.namespace + ":" + key
            self.kvs.remove(key)
        return defer.succeed(None)
