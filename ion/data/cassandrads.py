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
        self.client = pycassa.connect(cass_host_list)
        self.kvs = pycassa.ColumnFamily(self.client, 'Datasets', 'Catalog')
        logging.info('connected OK.')

    def get(self, key):
        try:
            val = self.kvs.get(key)
            logging.info('Key "%s":"%s"' % (key, val))
            return(val['value'])
        except:
            logging.info('Key "%s" not found' % key)
            return(None)

    def put(self, key, value):
        logging.info('writing key %s value %s' % (key, value))
        self.kvs.insert(key, {'value' : value})
        logging.info('write complete')

    def query(self, regex):
        matched_list = []
        try:
            klist = self.kvs.get_range()
            for x in klist:
                if re.search(regex, x[0]):
                    matched_list.append(x)
            return(matched_list)
        except:
            logging.error('Unable to find any keys')
            return None

    def delete(self, key):
        try:
            self.kvs.remove(key)
        except:
            logging.warn('Error removing key')
