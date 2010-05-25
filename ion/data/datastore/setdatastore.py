#!/usr/bin/env python

"""
@file ion/data/datastore/setDataStore.py
@author Paul Hubbard
@author Matt Rodriguez
@author David Stuebe
@brief client interface for storing and retrieving stateful data objects.
@note Depends on pycassa being installed and the instance on amoeba being up.
"""

import logging
import pycassa


class SetCassandraStore():
    """
    Provides a set data structure abstraction for interacting with the
    Cassandra data store.
    """
    def __init__(self, cass_host_list=None):
        """
        @brief Constructor, safe to use no arguments
        @param cass_host_list List of hostname:ports for cassandra host
        or cluster
        @retval Connected object instance
        """
        if not cass_host_list:
            logging.info('Connecting to Cassandra on localhost...')
        else:
            hosts = str(cass_host_list)
            logging.info('Connecting to Cassandra at "%s"...' % hosts)
        client = pycassa.connect(cass_host_list)
        self._kvs = pycassa.ColumnFamily(client, 'Datasets', 'Catalog')
        logging.info('connected OK.')

    def smembers(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key Cassandra key
        @retval Value from the ion dictionary, or None.
        The value returned can be a python dictionary, a python set, or
        an integer, float or string.
        """
        try:
            val = self._kvs.get(key)
            logging.info('Read Key:Val "%s":"%s"' % (key, val))
            return set(val)
        except:
            logging.info('Get: Key "%s" not found' % key)
            return None

    def sadd(self, key, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value. The value can be a python Dictionary
        a python Set, or an integer, float or string.
        @retval None

        The builds the set abstraction on top of the dictionary structure,
        by requiring the column name to be the same as the value. This
        ensures that key has a collection of unique items.
        @note the timestamp is updated when the value is added.
        """
        logging.info('writing key %s value %s' % (key, value))
        col = {value: value}
        self._kvs.insert(key, col)
        logging.info('write complete')

    def sremove(self, key, value):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @param value, the value to remove from the set
        @retval None
        """
        try:
            self._kvs.remove(key, [value])
        except:
            logging.warn("Error removing key")

    def scard(self, key):
        """
        @brief return the number of elements in the set
        @param key which is mapped to the set
        @retval card an int representing the the cardinality of the set
        """
        try:
            vals = self._kvs.get(key)
            return len(vals)
        except:
            logging.warn("Error calculating cardinality")

    def delete(self, key):
        """
        @brief remove the entire set and key from the data store
        @param key which is mapped to the set
        """
        try:
            self._kvs.remove(key)
        except:
            logging.warn("Error removing set")
