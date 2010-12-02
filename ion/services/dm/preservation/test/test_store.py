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

from ion.data.backends import cassandra
from ion.services.dm.preservation.store import Store

from ion.data.backends import irodsstore 

from ion.data.backends.store_service import StoreServiceClient
from ion.test.iontest import IonTestCase


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
        d = Store.create_store()
        return d
    

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.clear_store()


    @defer.inlineCallbacks
    def test_clear_store(self):
        # Make sure we can't read the cleared store
        yield self.ds.put(self.key, self.value)
        rc = yield self.ds.clear_store()
        if rc == None:
            log.info("Success clearing store!")
        rc = yield self.ds.get(self.key)
        self.failUnlessEqual(rc, None)


    @defer.inlineCallbacks
    def test_get_404(self):
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

    @defer.inlineCallbacks
    def test_query(self):
        # Write a key, query for it, verify contents
        yield self.ds.put(self.key, self.value)
        rl = yield self.ds.query(self.key)
#        print 'type rl',type(rl)
#        print 'type rl[0]', type(rl[0])
#        print 'type rl[0][0]', type(rl[0][0])
        self.failUnlessEqual(rl[0], self.key)



class CassandraStoreTestSuperCols(IStoreTest):

    def _setup_backend(self):
        clist = ['amoeba.ucsd.edu:9160']
        d = cassandra.CassandraStore.create_store(cass_host_list=clist)
        return d
    
    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.clear_store()
        self.ds.manager.shutdown()


class CassandraStoreTestNoSuperCols(IStoreTest):

    def _setup_backend(self):
        clist = ['amoeba.ucsd.edu:9160']
        ds = cassandra.CassandraStore.create_store(
            cass_host_list=clist,
            cf_super=False,
            keyspace='Datasets',
            colfamily='Catalog'
            )
        return ds
        
    @defer.inlineCallbacks
    def tearDown(self):
        log.info("in tearDown")
        yield self.ds.clear_store()
        log.info("Calling shutdown")
        self.ds.manager.shutdown()
        log.info("Shutdown")
    
    @defer.inlineCallbacks  
    def test_clear_store(self):
        """
        @note This doesn't work if I just raise an exception. The problem
        seems to be that shutdown is called on the protocol factory before
        a connection has been made. This might be a bug with the Telephus
        client. I'll investigate further after I get this merged back 
        into the main branch
        
        Matt Rodriguez 9/30/10
        """
        yield self.ds.put(self.key, self.value)
        yield self.ds.remove(self.key)
        #import twisted.internet.base
        #twisted.internet.base.DelayedCall.debug = True
        #raise unittest.SkipTest('Can not clear the persistent store if the name space is not unique')
    
    
        
    

class CassandraStoreTestSup(IStoreTest):

    def _setup_backend(self):
        clist = ['amoeba.ucsd.edu:9160']
        ds = cassandra.CassandraStore.create_store(
            cass_host_list=clist,
            keyspace='Datastore',
            colfamily='DS1',
            cf_super=True,
            namespace='n')
        return ds
    
    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.clear_store()
        self.ds.manager.shutdown()

class CassandraSuperStoreRandomNameSpaceTest(IStoreTest):

    def _setup_backend(self):
        clist = ['amoeba.ucsd.edu:9160']
        ds = cassandra.CassandraStore.create_store(
            cass_host_list=clist,
            keyspace='DatastoreTest',
            colfamily='DS1',
            cf_super=True)
        return ds

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.clear_store()
        self.ds.manager.shutdown()
        
class IRODSStoreTest(IStoreTest):
    
    def _setup_backend(self):
        irods_config = {'irodsHost': 'ec2-204-236-137-245.us-west-1.compute.amazonaws.com', \
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
               
    def test_query(self):
        """
        @note This isn't implemented in the IRODS store.
        The query functionality is deprecated in the other store implementations
        """
        pass
        
class StoreServiceTest(IonTestCase, IStoreTest):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def _setup_backend(self):
        yield self._start_container()
        # By default, the store service will use Store in the backend.
        services = [
            {'name':'store1','module':'ion.data.backends.store_service','class':'StoreService'},
        ]
        self._sup = yield self._spawn_processes(services)
        ds = yield StoreServiceClient.create_store(proc=self._sup)
        defer.returnValue(ds)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.ds.clear_store()
        log.info("dir(self.ds) %s " % self.ds.__dict__)
        yield self._shutdown_processes(proc=self._sup)
        yield self._stop_container()



