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
from ion.core.data.irodsstore import IrodsStore
from twisted.internet import defer


class TestIrodsDataStoreInterface(unittest.TestCase):
    def setUp(self):
        # This will use the iRODS config info in 'ion.config'
        # self.ds = IrodsStore.create_store()

        # here we use config through args
        #'ec2-204-236-137-245.us-west-1.compute.amazonaws.com'
        irods_config = {'irodsHost': 'ec2-204-236-159-249.us-west-1.compute.amazonaws.com', \
                    'irodsPort':'1247', \
                    'irodsDefResource':'ooi-test-resc1', \
                    'irodsOoiCollection':'/ooi-test-cluster1/home/testuser/OOI', \
                    'irodsUserName':'testuser', \
                    'irodsUserPasswd':'test', \
                    'irodsZone':'ooi-test-cluster1'}
        ds = IrodsStore.create_store(**irods_config)
        self.ds = ds.result

        self.key = self._mkey()
        self.value = self._mkey()

    def tearDown(self):
        self.ds.clear_store()

    def _mkey(self):
        # Generate a pseudo-random string. handy, that.
        return str(uuid4())

    def test_irods(self):
        self.ds.put(self.key, self.value)
        value = self.ds.get(self.key)

