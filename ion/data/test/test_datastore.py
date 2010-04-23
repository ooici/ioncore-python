#!/usr/bin/env python

"""
@file ion/data/test/test_datastore.py
@author Paul Hubbard
@test Service only test of Cassandra datastore
"""


from twisted.trial import unittest
import logging
from uuid import uuid4

from ion.data.cassandrads import CassandraStore

class DatastoreTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')
        clist = ['amoeba.ucsd.edu:9160']
        self.ds = CassandraStore(cass_host_list=clist)
        self.ds.start()

    def tearDown(self):
        del self.ds

    def _mkey(self):
        return str(uuid4())

    def test_get_404(self):
        rc = self.ds.get(self._mkey())
        self.failUnlessEqual(rc, None)

    def test_write_only(self):
        self.ds.put(self._mkey(), self._mkey())

    def test_putget(self):
        key = self._mkey()
        value = 'val1'
        self.ds.put(key, value)
        b = self.ds.get(key)
        self.failUnlessEqual(value, b)
