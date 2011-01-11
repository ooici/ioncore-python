"""
@file ion/core/data/test/test_manager.py
@author Matt Rodriguez
@test Test the IDataManager interface
"""
from twisted.trial import unittest
from twisted.internet import defer

from ion.core.data.cassandra import CassandraDataManager, CassandraStorageResource

from ion.core.object import workbench
from net.ooici.storage import persistent_archive_pb2

class IDataManagerTest(unittest.TestCase):
    
    @defer.inlineCallbacks
    def setUp(self):
        self.wb = workbench.WorkBench('No Process: Testing only')
        self.manager = yield self._setUpConnection()
        self._setUpArchiveAndCache()
        
    def test_instantiate(self):
        pass
    
    def test_create_persistent_archive(self):
        
        self.manager.create_persistent_archive(self.keyspace)
    
    
class CassandraDataManagerTest(IDataManagerTest):
    
    
    def _setUpArchiveAndCache(self):
        
        ### Create a Persistent Archive resource - for cassandra a Cassandra KeySpace object
        persistent_archive_repository, cassandra_keyspace  = self.wb.init_repository(persistent_archive_pb2.CassandraKeySpace)
        # only the name of the keyspace is required
        cassandra_keyspace.name = 'ManagerTestKeyspace'
        #cassandra_keyspace.name = 'Keyspace1'
        self.keyspace = cassandra_keyspace
        
    
    def _setUpConnection(self):
        
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        persistent_technology_repository, cassandra_cluster  = self.wb.init_repository(persistent_archive_pb2.CassandraCluster)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        cas_host.host = 'ec2-204-236-159-249.us-west-1.compute.amazonaws.com'
        cas_host.port = 9160
        
        ### Create a Credentials resource - for cassandra a SimplePassword object
        cache_repository, simple_password  = self.wb.init_repository(persistent_archive_pb2.SimplePassword)
        simple_password.username = 'ooiuser'
        simple_password.password = 'oceans11'
        
        storage_resource = CassandraStorageResource(cassandra_cluster, credentials=simple_password)
        manager = CassandraDataManager(storage_resource)  
        
        manager.initialize() 
        manager.activate()
        return defer.succeed(manager)
    
    #def tearDown(self):
        