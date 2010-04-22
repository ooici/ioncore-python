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
        self.kvs.insert(key, {'value' : str(value)})

    def query(self, regex):
        logging.error('Missing code')
        return None

    def delete(self, key):
        logging.error('Missing code')
        return None


def pfh_test():
    start()
    receive({'op':'put','content':{'key':'key1','value':'val1'}}, None)
    receive({'op':'get','content':{'key':'key1'}}, None)
