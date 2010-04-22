#!/usr/bin/env python

"""
@file ion/data/cassandrads.py
@author Michael Meisinger
@author Paul Hubbard
@brief client interface for storing and retrieving stateful data objects.
"""

import logging
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import pycassa


class CassandraStore():
    """
    Store interface for interacting with the Cassandra key/value store
    @see http://github.com/vomjom/pycassa
    """
    def started(self):
        return(hasattr(self, 'kvs'))

    def start(self):
        if self.started():
            return

        logging.info('Connecting to Cassandra...')
        cass_list = ['localhost:9160']
        self.client = pycassa.connect(cass_list)
        self.kvs = pycassa.ColumnFamily(self.client, 'Datasets', 'Catalog')
        logging.info('connected OK.')

    def get(self, key):
        if not self.started():
            logging.error('Not connected!')
            return None
        try:
            val = self.kvs.get(key)
            logging.info('Key "%s":"%s"' % (key, val))
            return(val['value'])
        except:
            logging.info('Key "%s" not found' % key)
            return(None)

    def put(self, key, value):
        if not self.started():
            logging.error('Not connected!')
            return None
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
