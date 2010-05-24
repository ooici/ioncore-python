#!/usr/bin/env python
"""
@file ion/data/backends/cassandra.py
@author Michael Meisinger
@author Paul Hubbard
@author Dorian Raymer
@brief Implementation of ion.data.store.IStore using pycassa to interface a
Cassandra datastore backend
"""

import re
import logging

from twisted.internet import defer

import pycassa

from ion.data import store


class CassandraStore(store.IStore):
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    """
    def __init__(self, cass_host_list=None):
        self.kvs=None
        self.cass_host_list = cass_host_list

    def _init(self, cass_host_list):
        """
        @brief Constructor, if no args then connect to localhost
        @param cass_host_list List of hostname:ports for cassandra host or cluster
        """
        if not cass_host_list:
            logging.debug('Connecting to Cassandra on localhost...')
        else:
            logging.debug('Connecting to Cassandra at "%s"...' % str(cass_host_list))
        client = pycassa.connect(cass_host_list)
        # @note Hardwired to column family in cassandra install!
        self.kvs = pycassa.ColumnFamily(client, 'Datasets', 'Catalog')
        logging.info('connected OK.')
        return True

    def init(self):
        """
        @brief User-level constructor, uses data from self._init
        @retval Deferred
        """
        #return defer.maybeDeferred(self._init, cass_host_list, None)
        return defer.succeed(self._init(self.cass_host_list))

    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key Cassandra key
        @retval Value from the ion dictionary, or None
        """
        value = None
        try:
            val = self.kvs.get(key)
            logging.info('Key "%s":"%s"' % (key, val))
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
        logging.info('writing key %s value %s' % (key, value))
        self.kvs.insert(key, {'value':value})
        logging.info('write complete')
        return defer.succeed(None)

    def query(self, regex):
        """
        @brief Search by regular expression
        @param regex Regular expression to match against the keys
        @retval List, possibly empty, of keys that match.
        @note Uses get_range generator of unknown efficiency.
        """
        matched_list = []
        klist = self.kvs.get_range()
        for x in klist:
            if re.search(regex, x[0]):
                matched_list.append(x)
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
