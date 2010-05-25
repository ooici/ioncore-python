#!/usr/bin/env python
"""
@file ion/data/backends/cassandra.py
@author Paul Hubbard
@author Michael Meisinger
<<<<<<< HEAD
=======
@author Paul Hubbard
>>>>>>> 617e221edbc1767a3e0a36253b29e39cb8f94dfb
@author Dorian Raymer
@brief Implementation of ion.data.store.IStore using pycassa to interface a
        Cassandra datastore backend
"""

import re
import logging

from twisted.internet import defer

import pycassa

from ion.data.store import IStore


class CassandraStore(IStore):
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    """
    def __init__(self, **kwargs):
        self.kvs = None
        self.cass_host_list = None

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
        if not inst.cass_host_list:
            logging.info('Connecting to Cassandra on localhost...')
        else:
            logging.info('Connecting to Cassandra at "%s"...' % str(inst.cass_host_list))
        inst.client = pycassa.connect(inst.cass_host_list)
        inst.kvs = pycassa.ColumnFamily(inst.client, 'Datasets', 'Catalog')
        logging.info('connected OK.')
        return defer.succeed(inst)

    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key Cassandra key
        @retval Value from the ion dictionary, or None
        """
        value = None
        try:
            val = self.kvs.get(key)
            #logging.info('Key "%s":"%s"' % (key, val))
            value = val['value'] #this could fail if insert did it wrong
        except pycassa.NotFoundException:
            logging.info('Key "%s" not found' % key)
        return defer.succeed(value)

    def put(self, key, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value
        @note Value is composed into OOI dictionary under keyname 'value'
        @retval None
        """
        #logging.info('writing key %s value %s' % (key, value))
        self.kvs.insert(key, {'value':value})
        #logging.info('write complete')
        return defer.succeed(None)

    def query(self, regex):
        """
        @brief Search by regular expression
        @param regex Regular expression to match against the keys
        @retval List, possibly empty, of keys that match.
        @note Uses get_range generator of unknown efficiency.
        """
        #@todo This implementation is totally inefficient. MUST replace.
        matched_list = []
        klist = self.kvs.get_range()
        for x in klist:
            if re.search(regex, x[0]):
                matched_list.append(x[0])
        return defer.succeed(matched_list)

    def delete(self, key):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval None
        @note Deletes are lazy, so key may still be visible for some time.
        """
        # Only except on specific exceptions.
        #try:
        #    self.kvs.remove(key)
        #except: # Bad to except on anything and not re-raise!!
        #    logging.warn('Error removing key')
        #    return defer.fail()
        self.kvs.remove(key)
        return defer.succeed(None)
