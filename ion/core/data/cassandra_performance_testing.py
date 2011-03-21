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
        if self.index:
            self.store = CassandraIndexedStoreBootstrap("ooiuser", "oceans11")
        else:
            self.store = CassandraStoreBootstrap("ooiuser", "oceans11")
        
        self.num_rows = num_rows
        self.blobs = {}
        
        f = open("/dev/urandom")  
        t1 = time.time()
        for i in range(self.num_rows):
            blob = f.read(blob_size)
            key = sha.sha(blob).digest()
            self.blobs.update({key:blob})
        t2 = time.time()
        diff = t2 - t1
        print "Time creating blobs %s " % (diff,)
        
        #Have the store connect to the Cassandra cluster
        self.store.initialize()
        self.store.activate()
        self.indexes = []
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
        """
        The first index has a cardinality of 1, the second has a cardinality of 2, and 
        the 12th has a cardinality of 12. 
        """
        self.indexes = ["branch_name","keyword","object_branch",
                   "object_commit","object_key","predicate_branch",
                   "predicate_commit","predicate_key","repository_key",
                   "subject_branch","subject_commit","subject_key" ]
        
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
           
    def get_key_fraction(self, keys_fraction):
        
        keys = self.blobs.keys()
        random.shuffle(keys)
        test_keys = []
        num_has_keys = int(round(len(keys)* keys_fraction))
        for i in range(num_has_keys):
            test_keys.append(keys.pop())
            
        return test_keys
        
    @defer.inlineCallbacks
    def gets(self):
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
  
    
    
    
def main():
    parser = OptionParser()
    parser.add_option("-b", "--blobs", dest="blobs", default=100, help="The number of blobs or rows to put into Cassandra")
    parser.add_option("-s", "--size", dest="size", default=MB, help="The number of blobs or rows to put into Cassandra")
    opts, args = parser.parse_args()

    tester = CassandraPerformanceTester(index=True,num_rows=int(opts.blobs),blob_size=int(opts.size))
    tester.runTests()
    reactor.run() 
    
if __name__ == "__main__":
    main()
    
