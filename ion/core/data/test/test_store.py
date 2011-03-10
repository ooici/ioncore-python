#!/usr/bin/env python

"""
@file ion/data/test/test_store.py
@author Paul Hubbard
@author Dorian Raymer
@author David Stuebe
@author Matt Rodriguez
@test Service test of IStore Implementation

@TODO - Right now skiptest causes an error when used with a cassandra connection
 Once this is fixed we can skip individual tests. For now we must skip all or none
 by skipping the setUp or a method inside it!

"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.core.data import store
from ion.core.data import cassandra
from ion.core.data import index_store_service

# Import the workbench and the Persistent Archive Resource Objects!
from ion.core.object import workbench

from ion.core.object import object_utils
from ion.core.data.store import Query

from ion.core import ioninit
CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

from ion.core.data.cassandra_bootstrap import CassandraStoreBootstrap, CassandraIndexedStoreBootstrap

simple_password_type = object_utils.create_type_identifier(object_id=2502, version=1)
columndef_type = object_utils.create_type_identifier(object_id=2508, version=1)
column_family_type = object_utils.create_type_identifier(object_id=2507, version=1)
cassandra_cluster_type = object_utils.create_type_identifier(object_id=2504, version=1)
cassandra_keyspace_type = object_utils.create_type_identifier(object_id=2506, version=1)


        

class IStoreTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 10
        self.ds = yield self._setup_backend()

        # Test strings
        #self.key = str(uuid4())
        #self.value = str(uuid4())

        # Test Bytes
        self.key = object_utils.sha1bin(str(uuid4()))
        self.value = object_utils.sha1bin(str(uuid4()))
        defer.returnValue(None)


    def _setup_backend(self):
        """return a deferred which returns a initiated instance of a
        backend
        """
        return defer.maybeDeferred(store.Store)

    #def test_instantiate(self):
    #    pass

    @defer.inlineCallbacks
    def test_get_none(self):
        # Make sure we can't read the not-written
        rc = yield self.ds.get(self.key)
        self.assertEqual(rc, None)

    @defer.inlineCallbacks
    def test_write_and_delete(self):
        # Hmm, simplest op, just looking for exceptions
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)
        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_delete(self):
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)
        defer.returnValue(None)
        
    @defer.inlineCallbacks
    def test_put_get_delete(self):
        # Write, then read to verify same
        yield self.ds.put(self.key, self.value)
        b = yield self.ds.get(self.key)
        self.failUnlessEqual(self.value, b)
        yield self.ds.remove(self.key)
        defer.returnValue(None)

class BootstrapStoreTest(IStoreTest):
    
    @itv(CONF)
    def _setup_backend(self):
        store = CassandraStoreBootstrap("ooiuser", "oceans11")
        store.initialize()
        store.activate()
        return defer.succeed(store)
        
    @defer.inlineCallbacks
    def tearDown(self):
        try:
            yield self.ds.terminate()
        except Exception, ex:
            log.info("Exception raised in tearDown %s" % (ex,))    
            
class BootstrapIndexedStoreTest(IStoreTest):
    
    @itv(CONF)
    def _setup_backend(self):
        store = CassandraIndexedStoreBootstrap("ooiuser", "oceans11")
        store.initialize()
        store.activate()
        return defer.succeed(store)
        
    @defer.inlineCallbacks
    def tearDown(self):
        try:
            yield self.ds.terminate()
        except Exception, ex:
            log.info("Exception raised in tearDown %s" % (ex,))               

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
        persistent_archive_repository, cassandra_keyspace  = wb.init_repository(cassandra_keyspace_type)
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


    
    @defer.inlineCallbacks
    def tearDown(self):
        try:
            yield self.ds.terminate()
        except Exception, ex:
            log.info("Exception raised in tearDown %s" % (ex,))
            
            
class IndexStoreTest(IStoreTest):

    @defer.inlineCallbacks
    def setUp(self):
        yield IStoreTest.setUp(self)
        yield self.put_stuff_for_tests()
        defer.returnValue(None)
        
    def _setup_backend(self):
        """return a deferred which returns a initiated instance of a
        backend
        """

        ds = store.IndexStore(indices=['full_name', 'state', 'birth_date'])

        return defer.succeed(ds)
    
    @defer.inlineCallbacks
    def test_get_query_attributes(self):
        attrs = yield self.ds.get_query_attributes()
        log.info("attrs %s" % (attrs,))
        attrs_set = set(attrs)
        correct_set = set(['full_name', 'state', 'birth_date'])
        self.failUnlessEqual(attrs_set, correct_set)
        defer.returnValue(None)


    # Test a single query, single result
    @defer.inlineCallbacks
    def test_query_single(self):

        
        query = Query()
        query.add_predicate_eq('birth_date', '1973')
        rows = yield self.ds.query(query)
        log.info("Rows returned %s " % (rows,))
        self.assertEqual(rows['prothfuss']['value'], self.binary_value2)
        self.assertEqual(len(rows),1)
        for key in self.d2.keys():
            self.assertIn(key, rows['prothfuss'])
        
        defer.returnValue(None)
    # Test a single query, multiple result
    @defer.inlineCallbacks
    def test_query_single_2(self):

        query = Query()
        query.add_predicate_eq('state', 'UT')
        rows = yield self.ds.query(query)
        log.info("Rows returned %s " % (rows,))
        self.assertEqual(rows['bsanderson']['value'], self.binary_value1)
        self.assertEqual(rows['htayler']['value'], self.binary_value3)
        self.assertEqual(rows['jstewart']['value'], self.binary_value4)
        self.assertEqual(len(rows),3)
        for key in self.d1.keys():
            self.assertIn(key, rows['bsanderson'])

        for key in self.d3.keys():
            self.assertIn(key, rows['htayler'])

        for key in self.d4.keys():
            self.assertIn(key, rows['jstewart'])

            
   
    # Tests multiple atts
    @defer.inlineCallbacks
    def test_query_multiple(self):
        
        query = Query()
        query.add_predicate_eq('birth_date','1973')
        query.add_predicate_eq('state','WI')
        rows = yield self.ds.query(query)
        log.info("Rows returned %s " % (rows,))
        self.assertEqual(rows['prothfuss']['value'], self.binary_value2)
        self.assertEqual(len(rows),1)
        

    # Tests no result
    @defer.inlineCallbacks
    def test_query_no_resuluts(self):
        query = Query()
        
        query.add_predicate_eq('birth_date', '1978')
        query.add_predicate_eq('state', 'WI')
        rows = yield self.ds.query(query)
        log.info("Rows returned %s " % (rows,))
        self.assertEqual(len(rows),0)
        

    # Tests greater than 1970 and state == UT
    @defer.inlineCallbacks
    def test_query_greater_and_eq(self):

        query = Query()
        query.add_predicate_gt('birth_date','1970')
        query.add_predicate_eq('state','UT')
        rows = yield self.ds.query(query)

        log.info("Rows returned %s " % (rows,))
        self.assertEqual(len(rows),1)
        self.assertEqual(rows['bsanderson']['value'], self.binary_value1)
        for key in self.d1.keys():
            self.assertIn(key, rows['bsanderson'])

        
        
    # Tests greater than
    @defer.inlineCallbacks
    def test_query_greater_and_eq_2(self):
        
        query = Query()
        query.add_predicate_gt('birth_date','')
        query.add_predicate_eq('state','UT')
        
        rows = yield self.ds.query(query)

        log.info("Rows returned %s " % (rows,))
        self.assertEqual(len(rows),2)
        self.assertEqual(rows['bsanderson']['value'], self.binary_value1)
        self.assertEqual(rows['htayler']['value'], self.binary_value3)
        
        for key in self.d1.keys():
            self.assertIn(key, rows['bsanderson'])

        for key in self.d3.keys():
            self.assertIn(key, rows['htayler'])
        


        
        
    @defer.inlineCallbacks
    def put_stuff_for_tests(self):
        """
        helper method for loading some data to test the query functions
        """
        self.d1 = {'full_name':'Brandon Sanderson', 'birth_date': '1975', 'state':'UT'}
        self.d2 = {'full_name':'Patrick Rothfuss', 'birth_date': '1973', 'state':'WI'}
        self.d3 = {'full_name':'Howard Tayler', 'birth_date': '1968', 'state':'UT'}

        # Add one more that has no DOB
        self.d4 = {'full_name':'John Stewart', 'state':'UT'}

        self.binary_value1 = 'BinaryValue for Brandon Sanderson'
        self.binary_value2 = 'BinaryValue for Patrick Rothfuss'
        self.binary_value3 = 'BinaryValue for Howard Tayler'

        self.binary_value4 = 'BinaryValue for John Stewart'
        
        yield self.ds.remove('bsanderson')
        yield self.ds.remove('prothfuss')
        yield self.ds.remove('htayler')
        yield self.ds.remove('jstewart')

        yield self.ds.put('bsanderson',self.binary_value1, self.d1)
        yield self.ds.put('prothfuss',self.binary_value2, self.d2)
        yield self.ds.put('htayler',self.binary_value3, self.d3)

        yield self.ds.put('jstewart',self.binary_value4, self.d4)

        


    @defer.inlineCallbacks
    def test_put(self):

        val1 = yield self.ds.get('bsanderson')
        val2 = yield self.ds.get('prothfuss')
        val3 = yield self.ds.get('htayler')
        self.failUnlessEqual(val1, self.binary_value1)
        self.failUnlessEqual(val2, self.binary_value2)
        self.failUnlessEqual(val3, self.binary_value3)
        
        

    @defer.inlineCallbacks
    def test_update_index_blank(self):

        new_attrs = {'birth_date': '1969'}

        self.d4.update(new_attrs)

        yield self.ds.update_index('jstewart', new_attrs)
        query = Query()
        query.add_predicate_eq('birth_date', '1969')
        rows = yield self.ds.query(query)
        log.info("Rows returned %s " % (rows,))
        self.assertEqual(rows['jstewart']['value'], self.binary_value4)
        self.assertEqual(len(rows),1)

        for key in self.d4.keys():
            self.assertIn(key, rows['jstewart'])
        

    @defer.inlineCallbacks
    def test_update_index_existing(self):

        new_attrs = {'birth_date': '1969'}

        self.d2.update(new_attrs)
        log.info("Updating the index")
        yield self.ds.update_index('prothfuss', new_attrs)
        log.info("Done updating the index")
        query = Query()
        query.add_predicate_eq('birth_date', '1969')
        rows = yield self.ds.query(query)
        log.info("Rows returned %s " % (rows,))
        self.assertEqual(rows['prothfuss']['value'], self.binary_value2)
        self.assertEqual(len(rows),1)

        for key in self.d2.keys():
            self.assertIn(key, rows['prothfuss'])
        
        
    
    @defer.inlineCallbacks
    def test_update_index_value_error(self):

        new_attrs = {'value': '1969'}

        try:
            yield self.ds.update_index('prothfuss', new_attrs)

        except store.IndexStoreError:
            defer.returnValue(None)

        self.fail('Did not raise Index Store Error')
        


class IndexStoreServiceTest(IndexStoreTest, IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield IStoreTest.setUp(self)
        yield self.put_stuff_for_tests()
        defer.returnValue(None)
        
    @defer.inlineCallbacks
    def _setup_backend(self):
        """
        Start the service and setup the client to the backend for the test.
        """

        yield self._start_container()
        self.timeout = 30
        services = [
            {'name':'index_store_service','module':'ion.core.data.index_store_service','class':'IndexStoreService',
             'spawnargs':{'indices':['full_name', 'state', 'birth_date']}},

        ]
        sup = yield self._spawn_processes(services)
        client = index_store_service.IndexStoreServiceClient(proc=sup)

        defer.returnValue(client)


    @defer.inlineCallbacks
    def tearDown(self):
        log.info("In tearDown")

        yield self._shutdown_processes()
        yield self._stop_container()


class CassandraIndexedStoreTest(IndexStoreTest):

    @itv(CONF)
    @defer.inlineCallbacks
    def setUp(self):
        yield IStoreTest.setUp(self)
        yield self.put_stuff_for_tests()
        defer.returnValue(None)
    
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
        persistent_archive_repository, cassandra_keyspace  = wb.init_repository(cassandra_keyspace_type)
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
        
    
    @defer.inlineCallbacks  
    def tearDown(self):
        yield self.ds.terminate()
        
             



