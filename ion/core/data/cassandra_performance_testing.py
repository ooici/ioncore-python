"""
@file ion/core/data/cassandra_performance_testing.py
@author Matt Rodriguez
@brief Generates cassandra-cli commands given the storage.cfg file as input.

"""
from ion.core.data.cassandra_bootstrap import CassandraStoreBootstrap, CassandraIndexedStoreBootstrap
from ion.core.data.store import Query
from twisted.internet import defer
from twisted.internet import reactor

import sha, time
import random
from optparse import OptionParser

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

KB = 1024
MB = 1024 * 1024




class CassandraPerformanceTester:
    
    def __init__(self, index=False, num_rows = 100, blob_size=MB):
        self.index = index
        self.blob_size = blob_size
        if self.index:
            self.store = CassandraIndexedStoreBootstrap("ooiuser", "oceans11")
        else:
            self.store = CassandraStoreBootstrap("ooiuser", "oceans11")
        
        self.num_rows = num_rows
        self.blobs = {}
        
        f = open("/dev/urandom")  
        t1 = time.time()
        for i in range(self.num_rows):
            blob = f.read(self.blob_size)
            key = sha.sha(blob).digest()
            self.blobs.update({key:blob})
        t2 = time.time()
        diff = t2 - t1
        print "Time creating blobs %s " % (diff,)
        
        #Have the store connect to the Cassandra cluster
        self.store.initialize()
        self.store.activate()
        self.indexes = ["branch_name","keyword","object_branch",
                   "object_commit","object_key","predicate_branch",
                   "predicate_commit","predicate_key","repository_key",
                   "subject_branch","subject_commit","subject_key" ]
        self.index_values_dict = {}
        
        
    @defer.inlineCallbacks    
    def runTests(self):
        yield  self.setUp()
       
        if self.index:
            yield self.update_index()
            yield self.query()
            
        yield self.has_key()
        yield self.gets()    
        yield  self.tearDown()   
        
         
    @defer.inlineCallbacks
    def setUp(self):
        """
        This puts the blobs into the Cassandra cluster
        """
        t1 = time.time()
        for k,v in self.blobs.items():
            yield self.store.put(k,v)
        t2 = time.time()
        diff =  t2 - t1
        print "Time to do %s puts %s " % (len(self.blobs),diff)
    
    def setup_update_index(self):
        """
        The first index has a cardinality of 1, the second has a cardinality of 2, and 
        the 12th has a cardinality of 12. 
        """
        
        
        indexes_mod_dict = dict(zip(self.indexes, range(1, len(self.indexes) + 1)))
        
       
        f = open("/dev/urandom")
        for i in range(len(self.indexes)):
            blob = f.read(1024)
            key = sha.sha(blob).digest()
            self.index_values_dict.update({i:key})
            
        update_requests = []    
        for i in range(len(self.blobs)):
            attributes = {}
            for index in self.indexes:
                mod = indexes_mod_dict[index]
                attributes.update({index:self.index_values_dict[i % mod]})
            update_requests.append(attributes)
        
        update_requests_dict = dict(zip(self.blobs.keys(), update_requests))
        return update_requests_dict
    
    @defer.inlineCallbacks
    def query(self):
        """
        q1 will return 1/12 of the rows
        q2 will return all of the rows
        q3 will return no rows
        q4 will return 1/12 of the rows, but use a compound query
        
        TODO need to set up the indexes to test the queries for greater than. Right now I can 
        do one that returns all.
        """
        
        @defer.inlineCallbacks
        def run_query(query):
            num_preds = len(query.get_predicates())
            t1 = time.time()
            num_queries = 1
            for i in range(num_queries):
                rows = yield self.store.query(query)
            t2 = time.time()
            diff = t2 - t1
            print "%s rows returned." % (len(rows))
            print "Time to do %s queries with %s predicate that returns %s rows: %s" % (num_queries,num_preds,len(rows), diff)
        
        q1 = Query()
        q1.add_predicate_eq("subject_key", self.index_values_dict[0])
        yield run_query(q1)
        
        q2 = Query()
        q2.add_predicate_eq("branch_name", self.index_values_dict[0])
        #This query returns all of the rows and is really slow.
        #yield run_query(q2)
        
        q3 = Query()
        q3.add_predicate_eq("branch_name", self.index_values_dict[1])
        yield run_query(q3)
        q4 = Query()
        q4.add_predicate_eq("subject_key", self.index_values_dict[0])
        q4.add_predicate_eq("branch_name", self.index_values_dict[1])
        yield run_query(q4)
        
        keys = self.blobs.keys()
        random.shuffle(keys)
        k1 = keys.pop()
        f = open("/dev/urandom")
        blob = f.read(1024)
        one_index_value = sha.sha(blob).digest()
        yield self.store.update_index(k1, {"subject_key": one_index_value})
        
        q5 = Query()
        q5.add_predicate_eq("subject_key", one_index_value)
        yield run_query(q5)
        
        k2 = keys.pop()
        blob = f.read(1024)
        two_index_value = sha.sha(blob).digest()
        yield self.store.update_index(k1, {"subject_commit": two_index_value})
        yield self.store.update_index(k2, {"subject_commit": two_index_value})
        
        q6 = Query()
        q6.add_predicate_eq("subject_commit", two_index_value)
        yield run_query(q6)
    
    @defer.inlineCallbacks
    def update_index(self):
        update_requests_dict = self.setup_update_index()
        t1 = time.time()
        for k,v in update_requests_dict.items():
            yield self.store.update_index(k, v)
        t2 = time.time()
        diff = t2 - t1
        print "Time to do %s update_index calls %s " % (len(self.blobs), diff)    
        
    @defer.inlineCallbacks
    def has_key(self):
        """
        First test has_key on a fraction of the keys
        that are in the cluster. Then test has_key on 
        the same number of keys that are not in the cluster.
        """
        
        test_keys = self.get_key_fraction(.5)
        num_has_keys = len(test_keys)    
        t1 = time.time()
        for k in test_keys:
            yield self.store.has_key(k)
        t2 = time.time()
        
        diff = t2 - t1
        print "Time to do %s has_keys with positives: %s " % (len(test_keys), diff)    
             
        f = open("/dev/urandom")
        no_keys = []
        for i in range(num_has_keys):
            blob = f.read(1024)
            key = sha.sha(blob).digest()
            no_keys.append(key)
            
        t1 = time.time()    
        for k in no_keys:
            yield self.store.has_key(k)  
        t2 = time.time()
        diff = t2 - t1
        print "Time to do %s has_keys with negatives: %s " % (len(no_keys), diff)   
           
    def get_key_number(self, key_number):       
        """
        Return a list of length key_number of random keys
        """
        keys = self.blobs.keys()
        random.shuffle(keys)
        test_keys = []
        for i in range(key_number):
            test_keys.append(keys.pop())
        
        return test_keys
        
    def get_key_fraction(self, keys_fraction):
        """
        Return a list of random keys given a key_fraction between 0 and 1.
        """
        keys = self.blobs.keys()
        random.shuffle(keys)
        test_keys = []
        num_has_keys = int(round(len(keys)* keys_fraction))
        for i in range(num_has_keys):
            test_keys.append(keys.pop())
            
        return test_keys

    @defer.inlineCallbacks
    def gets(self):
        """
        Run a test the times how long it takes to get 1/2 of all of the keys in the
        cluster
        """
        test_keys = self.get_key_fraction(.5)
        t1 = time.time()
        for k in test_keys:
            val = yield self.store.get(k)
        t2 = time.time()
        diff = t2 - t1
        print "Time to do %s gets: %s " % (len(test_keys), diff)
                    
    @defer.inlineCallbacks
    def tearDown(self):
        keys = self.blobs.keys()
        t1 = time.time()
        for k in keys:
            yield self.store.remove(k)
        t2 = time.time()
        diff = t2 - t1
        print "Time to do %s removes %s " % (len(keys),diff)
        reactor.stop()
          
class CassandraBenchmarkTests(CassandraPerformanceTester):
    
    def __init__(self, index=False, num_rows = 100, blob_size=MB):
        CassandraPerformanceTester.__init__(self, index, num_rows, blob_size)
        
    def runTests(self):
        self.runBenchmarkTests()
            
    @defer.inlineCallbacks
    def runBenchmarkTests(self):
        yield self.setUp()
        
        yield self.get_benchmark(50)
        yield self.has_key_benchmark(50)
        yield self.put_benchmark(50)
        if self.index:
            yield self.update_index_benchmark(50)
        yield self.tearDown()     
 
        
    @defer.inlineCallbacks
    def put_benchmark(self, ops):
        
        
        d = {}
        f = open("/dev/urandom")  
        for i in range(ops):
            blob = f.read(self.blob_size)
            key = sha.sha(blob).digest()
            d.update({key:blob})
        
        for k,v in d.items():
            t1 = time.time()
            yield self.store.put(k,v)
            t2 = time.time()
            diff = t2 - t1
            print "Time to do put %s " % (diff,)  
            
        for k in d.keys():
            yield self.store.remove(k)      
            
    
    @defer.inlineCallbacks
    def update_index_benchmark(self, ops):
        update_requests_dict = self.setup_update_index()
        for i in range(ops):
            k,v = update_requests_dict.popitem()
            t1 = time.time()
            yield self.store.update_index(k,v)
            t2 = time.time()
            diff = t2 - t1
            print "Update index time: %s " % (diff,)
    

                
    @defer.inlineCallbacks
    def has_key_benchmark(self, ops):
        test_keys = self.get_key_number(ops)
        for key in test_keys:
            t1 = time.time()
            yield self.store.has_key(key)  
            t2 = time.time()
            diff = t2 - t1
            print "Has key time: %s " % (diff,) 
                     

    
    @defer.inlineCallbacks
    def get_benchmark(self, ops):
        test_keys = self.get_key_number(ops)
        
        for k in test_keys:
            t1 = time.time()
            val = yield self.store.get(k)
            t2 = time.time()
            diff = t2 - t1
            print "Time to do a get: %s " % ( diff,)
        
class CassandraQueryBenchmarks(CassandraPerformanceTester):
    
    def __init__(self, index=False, num_rows = 100, blob_size=MB):
        CassandraPerformanceTester.__init__(self, index, num_rows, blob_size)
    
    def runTests(self):
        self.runQueryBenchMarks()
        
    @defer.inlineCallbacks
    def setup_indexes(self, f, rows_returned):
        b1 = f.read(1024)
        b2 = f.read(1024)
        search_term1 =  sha.sha(b1).digest()
        search_term2 = sha.sha(b2).digest()
        index_values = [search_term1, search_term2, "1"]
        b_random = f.read(1024)
        random_values = [ sha.sha(b_random).digest() for i in range(9) ]
        index_values.extend(random_values)
        index_attributes = dict(zip(self.indexes, index_values))
        
        for i in range(rows_returned):
            k,v = self.query_blobs.popitem()
            yield self.store.put(k,v,index_attributes)
        
        defer.returnValue((search_term1, search_term2))
  
    @defer.inlineCallbacks
    def put_rows(self, f):
        b1 = f.read(1024)
        index_val = sha.sha(b1).digest()
        random_values = [ index_val for i in range(len(self.indexes))]
        index_attributes = dict(zip(self.indexes, random_values))
        for k,v in self.query_blobs.items():
            yield self.store.put(k,v,index_attributes)
    
    @defer.inlineCallbacks
    def runQuery(self, q, pred_type):
        for i in range(50):
            t1 = time.time()
            rows = yield self.store.query(q,row_count=1000)
            t2 = time.time()
            diff = t2 - t1
            print "Returns %s_rows in %s query %s " % (len(rows), diff, pred_type)
            
    
    @defer.inlineCallbacks
    def runQueries(self, terms):
        q1 = Query()
        q1.add_predicate_eq(self.indexes[0], terms[0])
        yield self.runQuery(q1, "1_EQ")
        
        q1.add_predicate_eq(self.indexes[1], terms[1])
        yield self.runQuery(q1, "2_EQ")
        q1.add_predicate_gt(self.indexes[2], "")
        yield self.runQuery(q1, "2EQ_1GT")
        q2 = Query()
        q2.add_predicate_eq(self.indexes[0], terms[0])
        q2.add_predicate_gt(self.indexes[2], "")
        yield self.runQuery(q2, "1EQ_1GT")  
    
    @defer.inlineCallbacks
    def runQueryBenchMarks(self):
        self.query_blobs = dict(**self.blobs)
        f = open("/dev/urandom")
        t1 = time.time()
        terms1 = yield self.setup_indexes(f,1)
        terms10 = yield self.setup_indexes(f,10)
        terms100 = yield self.setup_indexes(f,100)
        terms1000 = yield self.setup_indexes(f,1000)
    
        t2 = time.time()
        diff = t2 - t1
        print "Time to setup indexes: %s " % (diff,)
        t1 = time.time()
        yield self.put_rows(f)
        t2 = time.time()
        diff = t2 - t1
        print "Time to put rows: %s " % (diff,)
        yield self.runQueries(terms1)
        yield self.runQueries(terms10)
        yield self.runQueries(terms100)
        yield self.runQueries(terms1000)
        yield self.tearDown()  
        
            
    
def main():
    parser = OptionParser()
    parser.add_option("-b", "--blobs", dest="blobs", default=100, help="The number of blobs or rows to put into Cassandra")
    parser.add_option("-s", "--size", dest="size", default=MB, help="The number of blobs or rows to put into Cassandra")
    parser.add_option("-i", "--indexed", action="store_true", dest="indexed", default=False, help="Use the indexed column family or the nonindexed column family")
    opts, args = parser.parse_args()
    #tester = CassandraPerformanceTester(index=opts.indexed, num_rows=int(opts.blobs),blob_size=int(opts.size))
    #tester.runBenchmarkTests()
    tester = CassandraQueryBenchmarks(index=opts.indexed, num_rows=int(opts.blobs),blob_size=int(opts.size))
    tester.runQueryBenchMarks()
    reactor.run() 
    
if __name__ == "__main__":
    main()
    
