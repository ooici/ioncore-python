#!/usr/bin/env python

"""
@file ion/data/store.py
@author Michael Meisinger
@brief base interface for all key-value stores in the system and default
        in memory implementation
"""

import re
import logging

from twisted.internet import defer

import pycassa

class IStore(object):
    """
    Interface for all store backend implementations. All operations are returning
    deferreds and operate asynchronously.
    """
    def get(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for value associated with key, or None if not existing.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"


    def read(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def put(self, key, value):
        """
        @param key  an immutable key to be associated with a value
        @param value  an object to be associated with the key. The caller must
                not modify this object after it was
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def write(self, *args, **kwargs):
        return self.put(*args, **kwargs)

    def query(self, regex):
        """
        @param regex  regular expression matching zero or more keys
        @retval Deferred, for list of values for keys matching the regex
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def delete(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def init(self, **kwargs):
        """
        Configures this Store with arbitrary keyword arguments
        @param kwargs  any keyword args
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

class Store(IStore):
    """
    Memory implementation of an asynchronous store, based on a dict.
    """
    def __init__(self):
        self.kvs = {}

    def get(self, key):
        """
        @see IStore.get
        """
        return defer.maybeDeferred(self.kvs.get, key, None)

    def put(self, key, value):
        """
        @see IStore.put
        """
        return defer.maybeDeferred(self.kvs.update, {key:value})

    def query(self, regex):
        """
        @see IStore.query
        """
        return defer.maybeDeferred(self._query, regex)

    def _query(self, regex):
        return [re.search(regex,m).group() for m in self.kvs.keys() if re.search(regex,m)]

    def delete(self, key):
        """
        @see IStore.delete
        """
        return defer.maybeDeferred(self._delete, key)

    def _delete(self, key):
        del self.kvs[key]
        return
    
    def init(self, **kwargs):
        return defer.maybeDeferred(True)

class CassandraStore(IStore):
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    """
    def __init__(self, cass_host_list=None):
        self.kvs=None
        self.cass_host_list = cass_host_list

    def _init(self, cass_host_list):
        if not cass_host_list:
            logging.info('Connecting to Cassandra on localhost...')
        else:
            logging.info('Connecting to Cassandra at "%s"...' % str(cass_host_list))
        client = pycassa.connect(cass_host_list)
        self.kvs = pycassa.ColumnFamily(client, 'Datasets', 'Catalog')
        logging.info('connected OK.')
        return True

    def init(self):
        """
        @brief Constructor, safe to use no arguments
        @param cass_host_list List of hostname:ports for cassandra host or cluster
        @retval Connected object instance
        """
        #return defer.maybeDeferred(self._init, cass_host_list, None)
        return self._init(self.cass_host_list)
        

    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param Key Cassandra key
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

