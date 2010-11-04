#!/usr/bin/env python

"""
@file ion/data/backends/test/test_setdatastore.py
@author Bing Zhu
@test Service only test of the iRODS datastore
"""


from twisted.trial import unittest
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4
from ion.data.backends.irodsstore import IrodsStore


class TestIrodsDataStoreInterface(unittest.TestCase):
    def setUp(self):
        self.ds = IrodsStore.create_store()
        self.key = self._mkey()
        self.value = self._mkey()

    def tearDown(self):
        self.ds.clear_store()

    def _mkey(self):
        # Generate a pseudo-random string. handy, that.
        return str(uuid4())

    def test_irods(self):
        self.ds.put(self.key, self.value)
        self.ds.get(self.key)
        #time.sleep(1)
        #self.ds.remove(self.key)

