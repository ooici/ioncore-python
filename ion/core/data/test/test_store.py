#!/usr/bin/env python

"""
@file ion/data/test/test_store.py
@author Paul Hubbard
@author Dorian Raymer
@author David Stuebe
@author Matt Rodriguez
@test Service test of IStore Implementation
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
from twisted.internet import defer
from twisted.internet import reactor

from ion.core.data import store
from ion.core.data import cassandra
from ion.core.data import irodsstore

from ion.test.iontest import IonTestCase

# Import the workbench and the Persistent Archive Resource Objects!
from ion.core.object import workbench
from net.ooici.storage import persistent_archive_pb2


class IStoreTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.ds = yield self._setup_backend()
        self.key = str(uuid4())
        self.value = str(uuid4())

    def _setup_backend(self):
        """return a deferred which returns a initiated instance of a
        backend
        """
        return defer.maybeDeferred(store.Store)

    @defer.inlineCallbacks
    def test_get_none(self):
        # Make sure we can't read the not-written
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)

    @defer.inlineCallbacks
    def test_delete(self):
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    @defer.inlineCallbacks
    def test_put_get_delete(self):
        # Write, then read to verify same
        yield self.ds.put(self.key, self.value)
        b = yield self.ds.get(self.key)
        self.failUnlessEqual(self.value, b)
        yield self.ds.remove(self.key)


class CassandraStoreTest(IStoreTest):

    def _setup_backend(self):
        
        ### This is a short cut to use resource objects without a process 
        wb = workbench.WorkBench('No Process: Testing only')
        
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        persistence_technology_repository, cassandra_cluster  = wb.init_repository(persistent_archive_pb2.CassandraCluster)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        #cas_host.host = 'amoeba.ucsd.edu'
        #cas_host.host = 'localhost'
        cas_host.host = 'ec2-204-236-159-249.us-west-1.compute.amazonaws.com'
        cas_host.port = 9160
        
        ### Create a Persistent Archive resource - for cassandra a Cassandra KeySpace object
        persistent_archive_repository, cassandra_keyspace  = wb.init_repository(persistent_archive_pb2.CassandraKeySpace)
        # only the name of the keyspace is required
        cassandra_keyspace.name = 'TestKeyspace'
        #cassandra_keyspace.name = 'Keyspace1'
        
        ### Create a Credentials resource - for cassandra a SimplePassword object
        cache_repository, simple_password  = wb.init_repository(persistent_archive_pb2.SimplePassword)
        # only the name of the column family is required
        simple_password.username = 'ooiuser'
        simple_password.password = 'oceans11'
        
        ### Create a Cache resource - for cassandra a ColumnFamily object
        cache_repository, column_family  = wb.init_repository(persistent_archive_pb2.ColumnFamily)
        # only the name of the column family is required
        column_family.name = 'TestCF'
        
        
        store = cassandra.CassandraStore(cassandra_cluster, \
                                         cassandra_keyspace, \
                                         simple_password, \
                                         column_family)
        
        store.initialize()
        store.activate()
        
        
        return defer.succeed(store)

    def tearDown(self):
        
        self.ds.terminate()

class IRODSStoreTest(IStoreTest):
    
    def _setup_backend(self):
        irods_config = {'irodsHost': 'ec2-204-236-159-249.us-west-1.compute.amazonaws.com', \
                    'irodsPort':'1247', \
                    'irodsDefResource':'ooi-test-resc1', \
                    'irodsOoiCollection':'/ooi-test-cluster1/home/testuser/OOI', \
                    'irodsUserName':'testuser', \
                    'irodsUserPasswd':'test', \
                    'irodsZone':'ooi-test-cluster1'}
        ds = irodsstore.IrodsStore.create_store(**irods_config)
        return ds
     
    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.clear_store()
