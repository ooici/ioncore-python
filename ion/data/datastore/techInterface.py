#!/usr/bin/env python

"""
@file ion/data/datastore/techInterface.py
@author Michael Meisinger
@author Paul Hubbard
@author David Stuebe
@brief client interface for storing and retrieving stateful data objects.
@note Depends on pycassa being installed and the instance on amoeba being up.
"""

import logging
import re
import hashlib
import pycassa
from uuid import uuid4

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
        if not cass_host_list:
            logging.info('Connecting to Cassandra on localhost...')
        else:
            logging.info('Connecting to Cassandra at "%s"...' % str(cass_host_list))
        client = pycassa.connect(cass_host_list)
        self._kvs = pycassa.ColumnFamily(client, 'Datasets', 'Catalog')
        logging.info('connected OK.')

    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key Cassandra key
        @retval Value from the ion dictionary, or None. The value returned can be
        a python dictionary, a python set, or an integer, float or string.
        """
        try:
            val = self._kvs.get(key)
            logging.info('Read Key:Val "%s":"%s"' % (key, val))
            if '__value__' in val:
                r = val['__value__']
            elif '__set__' in val:
                r=set()
                for v in val:
                    if v!='__set__':
                        r.add(val[v])
            else:
                r = val
            return(r)
        except:
            logging.info('Get: Key "%s" not found' % key)
            return(None)

    def put(self, key, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value. The value can be a python Dictionary
        a python Set, or an integer, float or string.
        @retval None
        """
        logging.info('writing key %s value %s' % (key, value))
        if type(value) == type(dict()):
            if '__value__' in value:
                logging.error('The dictionary key "__value__" is reserved')
                raise Exception('The dictionary key "__value__" is reserved in cassandras put function')
            if '__set__' in value:
                logging.error('The dictionary key "__set__" is reserved')
                raise Exception('The dictionary key "__set__" is reserved in cassandras put function')
            self._kvs.insert(key, value)

        elif type(value) == type(set()):
            d=dict()
            for val in value:
                col=hashlib.sha224(val).hexdigest()
                d[str(col)]=val
            d['__set__']='True'

            self._kvs.insert(key, d)

        else:
            self._kvs.insert(key, {'__value__' : value})

        logging.info('write complete')


    def incr(self, key):
        '''
        @brief Increment the value of a counter
        @param key is the name of the counter
        @retval integer counter
        @note Note quiet atomic - a race condition is possible where two
        simultanious calls would each increment the counter and return the same
        value. Each call will always result in an increment though.
        This method is clearly inefficient.
        '''
        col=str(uuid4())
        self._kvs.insert(key, {col:'a'})
        return self._kvs.get_count(key)



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
