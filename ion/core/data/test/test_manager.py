"""
@file ion/core/data/test/test_manager.py
@author Matt Rodriguez
@test Test the IDataManager interface

@TODO - Right now skiptest causes an error when used with a cassandra connection
 Once this is fixed we can skip individual tests. For now we must skip all or none
 by skipping the setUp or a method inside it!

"""
from twisted.trial import unittest
from twisted.internet import defer

from ion.core.data.cassandra import CassandraDataManager, CassandraStorageResource

from ion.core.object import workbench

from ion.core.data import store

from ion.core import ioninit
CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.object import object_utils


simple_password_type = object_utils.create_type_identifier(object_id=2502, version=1)

columndef_type = object_utils.create_type_identifier(object_id=2503, version=1)

column_family_type = object_utils.create_type_identifier(object_id=2507, version=1)

cassandra_cluster_type = object_utils.create_type_identifier(object_id=2504, version=1)

cassandra_keypsace_type = object_utils.create_type_identifier(object_id=2506, version=1)




class IDataManagerTest(unittest.TestCase):
    
    @defer.inlineCallbacks
    def setUp(self):
        self.wb = workbench.WorkBench('No Process: Testing only')
        self.manager = yield self._setUpConnection()
        self._setUpArchiveAndCache()
        
    def _setUpConnection(self):
        return defer.maybeDeferred(store.DataManager)
    
    def _setUpArchiveAndCache(self):
        self.keyspace = None
        self.cache = None
        
    @defer.inlineCallbacks    
    def test_instantiate(self):
        yield 1
    
    @defer.inlineCallbacks
    def test_create_persistent_archive(self):
        yield self.manager.create_persistent_archive(self.keyspace)
        
    
    @defer.inlineCallbacks
    def test_remove_persistent_archive(self):
        yield self.manager.create_persistent_archive(self.keyspace)
        yield self.manager.remove_persistent_archive(self.keyspace)

    @defer.inlineCallbacks
    def test_create_cache(self):
        yield self.manager.create_persistent_archive(self.keyspace)
        yield self.manager.create_cache(self.keyspace, self.cache)
        
    
    @defer.inlineCallbacks
    def test_remove_cache(self):
        yield self.manager.create_persistent_archive(self.keyspace)
        yield self.manager.create_cache(self.keyspace, self.cache)
        yield self.manager.remove_cache(self.keyspace, self.cache)
    
class CassandraDataManagerTest(IDataManagerTest):
    
    
    def _setUpArchiveAndCache(self):
        
        ### Create a Persistent Archive resource - for cassandra a Cassandra KeySpace object
        persistent_archive_repository, cassandra_keyspace  = self.wb.init_repository(cassandra_keypsace_type)
        # only the name of the keyspace is required
        cassandra_keyspace.name = 'ManagerTestKeyspace'
        #cassandra_keyspace.name = 'Keyspace1'
        self.keyspace = cassandra_keyspace
        ### Create a Cache resource - for cassandra a ColumnFamily object
        cache_repository, column_family  = self.wb.init_repository(column_family_type)
        # only the name of the column family is required
        column_family.name = 'TestCF'
        self.cache = column_family
        self.cache_repository = cache_repository
        column = self.cache_repository.create_object(columndef_type)
        column.column_name = "state"
        column.validation_class = 'org.apache.cassandra.db.marshal.UTF8Type'
        #IndexType.KEYS is 0, and IndexType is an enum
        column.index_type = 0
        column.index_name = 'stateIndex'
        self.cache.column_metadata.add()
        self.cache.column_metadata[0] = column
        
    
    @itv(CONF)
    def _setUpConnection(self):
        """
        This creates the ion resource objects necessary that hold the information needed to connect
        to the Cassandra cluster.
        """
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        persistent_technology_repository, cassandra_cluster  = self.wb.init_repository(cassandra_cluster_type)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        cas_host.host = 'ec2-204-236-159-249.us-west-1.compute.amazonaws.com'
        cas_host.port = 9160
        
        ### Create a Credentials resource - for cassandra a SimplePassword object
        cache_repository, simple_password  = self.wb.init_repository(simple_password_type)
        simple_password.username = 'ooiuser'
        simple_password.password = 'oceans11'
        
        storage_resource = CassandraStorageResource(cassandra_cluster, credentials=simple_password)
        manager = CassandraDataManager(storage_resource)  
        
        manager.initialize() 
        manager.activate()
        return defer.succeed(manager)
    
    @defer.inlineCallbacks
    def tearDown(self):
        log.info("In tearDown")
        try:
            yield self.manager.remove_persistent_archive(self.keyspace)
        except Exception, ex:
            log.info("Exception raised %s " % (ex,))
        self.manager.terminate()
         
    
    #@itv(CONF)
    @defer.inlineCallbacks
    def test_update_persistent_archive(self):
        yield self.manager.create_persistent_archive(self.keyspace)
        self.keyspace.replication_factor=2
        self.keyspace.strategy_class='org.apache.cassandra.locator.SimpleStrategy'
        yield self.manager.update_persistent_archive(self.keyspace)
        
        desc = yield self.manager._describe_keyspace(self.keyspace.name)
        log.info("Description of keyspace %s" % (desc,))
        log.info("Replication factor %s" % (desc.replication_factor,))    
        self.failUnlessEqual(desc.replication_factor, 2)
        
    #@itv(CONF)
    @defer.inlineCallbacks
    def test_update_cache(self):
        self.cache.column_type= 'Standard'
        self.cache.comparator_type='org.apache.cassandra.db.marshal.BytesType'
        yield self.manager.create_persistent_archive(self.keyspace)    
        yield self.manager.create_cache(self.keyspace, self.cache)
        yield self.manager.update_cache(self.keyspace, self.cache)
        desc = yield self.manager._describe_keyspace(self.keyspace.name)
        log.info("Description of keyspace %s" % (desc,))
        log.info("column_metadata index_name %s " % (desc.cf_defs[0].column_metadata[0].index_name,))
        self.failUnlessEqual(desc.cf_defs[0].column_metadata[0].index_name, "stateIndex")
        
    #@itv(CONF)
    @defer.inlineCallbacks
    def test_update_cache_two_indexes(self):
        """
        The first index is defined in the _setUpArchiveAndCache method
        
        This test creates two indexes and tests to see if the two index_names
        it gets back from a describe_keyspace call are the same as the two 
        index_names that were created. 
        """
        self.cache.column_type= 'Standard'
        self.cache.comparator_type='org.apache.cassandra.db.marshal.BytesType'
        

        column = self.cache_repository.create_object(columndef_type)
        #column_repository, column  = self.wb.init_repository(persistent_archive_pb2.ColumnDef)
        column.column_name = "state2"
        column.validation_class = 'org.apache.cassandra.db.marshal.UTF8Type'
        #IndexType.KEYS is 0, and IndexType is an enum
        column.index_type = 0
        column.index_name = 'stateIndex2'
        self.cache.column_metadata.add()
        self.cache.column_metadata[1] = column
        
        yield self.manager.create_persistent_archive(self.keyspace)    
        yield self.manager.create_cache(self.keyspace, self.cache)
        yield self.manager.update_cache(self.keyspace, self.cache)
        desc = yield self.manager._describe_keyspace(self.keyspace.name)
        index_name1 = desc.cf_defs[0].column_metadata[0].index_name
        index_name2 = desc.cf_defs[0].column_metadata[1].index_name
        log.info("index_names %s,%s" % (index_name1, index_name2))
        index_name_set = set((index_name1, index_name2))
        correct_index_name_set = set(("stateIndex", "stateIndex2"))
        self.failUnlessEqual(correct_index_name_set, index_name_set)
        