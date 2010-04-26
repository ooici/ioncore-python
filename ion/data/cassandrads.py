#!/usr/bin/env python

"""
@file ion/data/cassandrads.py
@author Michael Meisinger
@author Paul Hubbard
@brief client interface for storing and retrieving stateful data objects.
"""

import logging
import re

import pycassa

class CassandraStore():
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    """
    def __init__(self, cass_host_list=None):
        """
        @brief Constructor, safe to use no arguments
        @param cass_host_list List of hostname:ports for cassandra host or cluster
        @retval Connected object instance
        """
        logging.info('Connecting to Cassandra at "%s"...' % str(cass_host_list))
        client = pycassa.connect(cass_host_list)
        self._kvs = pycassa.ColumnFamily(client, 'Datasets', 'Catalog')
        logging.info('connected OK.')

    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param Key Cassandra key
        @retval Value from the ion dictionary, or None
        """
        try:
            val = self._kvs.get(key)
            logging.info('Key "%s":"%s"' % (key, val))
            return(val['value'])
        except:
            logging.info('Key "%s" not found' % key)
            return(None)

    def put(self, key, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value
        @note Value is composed into OOI dictionary under keyname 'value'
        @retval None
        """
        logging.info('writing key %s value %s' % (key, value))
        self._kvs.insert(key, {'value' : value})
        logging.info('write complete')

    def query(self, regex):
        """
        @brief Search by regular expression
        @param regex Regular expression to match against the keys
        @retval List, possibly empty, of keys that match.
        @note Uses get_range generator of unknown efficiency.
        """
        matched_list = []
        try:
            klist = self._kvs.get_range()
            for x in klist:
                if re.search(regex, x[0]):
                    matched_list.append(x)
            return(matched_list)
        except:
            logging.error('Unable to find any keys')
            return None

    def delete(self, key):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval None
        @note Deletes are lazy, so key may still be visible for some time.
        """
        try:
            self._kvs.remove(key)
        except:
            logging.warn('Error removing key')
