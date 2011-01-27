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

from ion.core.data import store
from ion.core.data import cassandra

# Import the workbench and the Persistent Archive Resource Objects!
from ion.core.object import workbench

from ion.core.object import object_utils

from ion.core import ioninit
CONF = ioninit.config(__name__)
from ion.util.test_decorator import itv


simple_password_type = object_utils.create_type_identifier(object_id=2502, version=1)
columndef_type = object_utils.create_type_identifier(object_id=2503, version=1)
column_family_type = object_utils.create_type_identifier(object_id=2507, version=1)
cassandra_cluster_type = object_utils.create_type_identifier(object_id=2504, version=1)
cassandra_keypsace_type = object_utils.create_type_identifier(object_id=2506, version=1)

class IStoreTest(unittest.TestCase):

    @itv(CONF)
    @defer.inlineCallbacks
    def setUp(self):
        self.ds = yield self._setup_backend()
        self.key = str(uuid4())
        self.value = str(uuid4())

    @itv(CONF)
    def _setup_backend(self):
        """return a deferred which returns a initiated instance of a
        backend
        """
        return defer.maybeDeferred(store.Store)

    @itv(CONF)
    @defer.inlineCallbacks
    def test_get_none(self):
        # Make sure we can't read the not-written
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    @itv(CONF)
    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)

    @itv(CONF)
    @defer.inlineCallbacks
    def test_delete(self):
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)

    @itv(CONF)
    @defer.inlineCallbacks
    def test_put_get_delete(self):
        # Write, then read to verify same
        yield self.ds.put(self.key, self.value)
        b = yield self.ds.get(self.key)
        self.failUnlessEqual(self.value, b)
        yield self.ds.remove(self.key)


class CassandraStoreTest(IStoreTest):

    @itv(CONF)
    def _setup_backend(self):
        
        ### This is a short cut to use resource objects without a process 
        wb = workbench.WorkBench('No Process: Testing only')
        
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        persistence_technology_repository, cassandra_cluster  = wb.init_repository(cassandra_cluster_type)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        #cas_host.host = 'amoeba.ucsd.edu'
        #cas_host.host = 'localhost'
        cas_host.host = 'ec2-204-236-159-249.us-west-1.compute.amazonaws.com'
        cas_host.port = 9160
        
        ### Create a Persistent Archive resource - for cassandra a Cassandra KeySpace object
        persistent_archive_repository, cassandra_keyspace  = wb.init_repository(cassandra_keypsace_type)
        # only the name of the keyspace is required
        cassandra_keyspace.name = 'StoreTestKeyspace'
        #cassandra_keyspace.name = 'Keyspace1'
        
        ### Create a Credentials resource - for cassandra a SimplePassword object
        cache_repository, simple_password  = wb.init_repository(simple_password_type)
        # only the name of the column family is required
        simple_password.username = 'ooiuser'
        simple_password.password = 'oceans11'
        
        ### Create a Cache resource - for cassandra a ColumnFamily object
        cache_repository, column_family  = wb.init_repository(column_family_type)
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
        try:       
            self.ds.terminate()
        except Exception, ex:
            log.info("Exception raised in tearDown %s" % (ex,))
            
            
            
class CassandraIndexedStoreTest(IStoreTest):

    @itv(CONF)
    def _setup_backend(self):
        """
        @note The column_metadata in the cache is not correct. The column family on the 
        server has a few more indexes.  
        """
        
        ### This is a short cut to use resource objects without a process 
        wb = workbench.WorkBench('No Process: Testing only')
        
        ### Create a persistence_technology resource - for cassandra a CassandraCluster object
        persistence_technology_repository, cassandra_cluster  = wb.init_repository(cassandra_cluster_type)
        
        # Set only one host and port in the host list for now
        cas_host = cassandra_cluster.hosts.add()
        #cas_host.host = 'amoeba.ucsd.edu'
        #cas_host.host = 'localhost'
        cas_host.host = 'ec2-204-236-159-249.us-west-1.compute.amazonaws.com'
        cas_host.port = 9160
        
        ### Create a Persistent Archive resource - for cassandra a Cassandra KeySpace object
        persistent_archive_repository, cassandra_keyspace  = wb.init_repository(cassandra_keypsace_type)
        # only the name of the keyspace is required
        cassandra_keyspace.name = 'StoreTestKeyspace'
        #cassandra_keyspace.name = 'Keyspace1'
        
        ### Create a Credentials resource - for cassandra a SimplePassword object
        cache_repository, simple_password  = wb.init_repository(simple_password_type)
        # only the name of the column family is required
        simple_password.username = 'ooiuser'
        simple_password.password = 'oceans11'
        
        ### Create a Cache resource - for Cassandra a ColumnFamily object

        cache_repository, column_family  = wb.init_repository(column_family_type)
        # only the name of the column family is required
        column_family.name = 'TestCF'
        
        self.cache = column_family
        self.cache_repository = cache_repository
        column = cache_repository.create_object(columndef_type)
        #column_repository, column  = wb.init_repository(columndef_type) # This is wrong...
        column.column_name = "state"
        column.validation_class = 'org.apache.cassandra.db.marshal.UTF8Type'
        #IndexType.KEYS is 0, and IndexType is an enum
        column.index_type = 0
        column.index_name = 'stateIndex'
        self.cache.column_metadata.add()
        self.cache.column_metadata[0] = column
        
        
        store = cassandra.CassandraIndexedStore(cassandra_cluster, \
                                                cassandra_keyspace, \
                                                simple_password, \
                                                column_family)
        
        store.initialize()
        store.activate()
        
        
        return defer.succeed(store)
        
    @itv(CONF)
    @defer.inlineCallbacks
    def test_get_query_attributes(self):
        attrs = yield self.ds.get_query_attributes()
        log.info("attrs %s" % (attrs,))
        attrs_set = set(attrs)
        correct_set = set(['full_name', 'state', 'birth_date'])
        self.failUnlessEqual(attrs_set, correct_set)

    @itv(CONF)
    @defer.inlineCallbacks
    def test_query(self):
        d1 = {'full_name':'Brandon Sanderson', 'birth_date': '1975', 'state':'UT'}
        d2 = {'full_name':'Patrick Rothfuss', 'birth_date': '1973', 'state':'WI'}     
        d3 = {'full_name':'Howard Tayler', 'birth_date': '1968', 'state':'UT'}
        binary_value1 = 'BinaryValue for Brandon Sanderson'
        binary_value2 = 'BinaryValue for Patrick Rothfuss'
        binary_value3 = 'BinaryValue for Howard Tayler'
        yield self.ds.put('bsanderson',binary_value1, d1)   
        yield self.ds.put('prothfuss',binary_value2, d2)   
        yield self.ds.put('htayler',binary_value3, d3) 
        query_attributes = {'birth_date':'1973'}
        rows = yield self.ds.query(query_attributes)
        log.info("Rows returned %s " % (rows,))
        self.failUnlessEqual(rows[0].key, 'prothfuss')
         
    @itv(CONF)
    @defer.inlineCallbacks
    def test_put(self):
        d1 = {'full_name':'Brandon Sanderson', 'birth_date': '1975', 'state':'UT'}
        d2 = {'full_name':'Patrick Rothfuss', 'birth_date': '1973', 'state':'WI'}     
        d3 = {'full_name':'Howard Tayler', 'birth_date': '1968', 'state':'UT'}
        binary_value1 = 'BinaryValue for Brandon Sanderson'
        binary_value2 = 'BinaryValue for Patrick Rothfuss'
        binary_value3 = 'BinaryValue for Howard Tayler'
        yield self.ds.put('bsanderson',binary_value1, d1)   
        yield self.ds.put('prothfuss',binary_value2, d2)   
        yield self.ds.put('htayler',binary_value3, d3)   
        val1 = yield self.ds.get('bsanderson')
        val2 = yield self.ds.get('prothfuss')
        val3 = yield self.ds.get('htayler')
        self.failUnlessEqual(val1, binary_value1)
        self.failUnlessEqual(val2, binary_value2)
        self.failUnlessEqual(val3, binary_value3)
    
    @defer.inlineCallbacks  
    def tearDown(self):
        yield self.ds.terminate()
             
