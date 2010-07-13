
import uuid


import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest

from ion.data import store
from ion.data.backends import store_service
from ion.data.backends import cassandra
from ion.data.datastore import registry
from ion.data import dataobject


from ion.test.iontest import IonTestCase
from twisted.internet import defer

from magnet.spawnable import Receiver
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class RegistryTest(unittest.TestCase):
    """
    """

    @defer.inlineCallbacks
    def setUp(self):
        s = yield self._set_up_backend()
#        s = yield cassandra.CassandraStore.create_store()
        self.reg = registry.Registry(s)
        self.mystore = s

    
    def _set_up_backend(self):
        s = store.Store.create_store()
        return (s)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.mystore.clear_store()


    @defer.inlineCallbacks
    def test_register(self):
        res = dataobject.ResourceDescription.create_new_resource()
        res.name = 'foo'
        #@Note - always over-write the old argument value!
        res = yield self.reg.register_resource_description(res)
        
        ref = res.reference()
        res2 = yield self.reg.get_resource_description(ref)
        #print res
        #print res2
        self.failUnless(res == res2)



    @defer.inlineCallbacks
    def test_register_overwrite(self):
        res = dataobject.ResourceDescription.create_new_resource()
        res.name = 'foo'
        res = yield self.reg.register_resource_description(res)
        ref1 = res.reference()
        # get this version back again
        res1 = yield self.reg.get_resource_description(ref1)
        self.failUnless(res == res1)
        self.assertEqual(res1.name, 'foo')
        
        #update with new data
        res.name = 'moo'
        res = yield self.reg.register_resource_description(res)

        # get the new version back again
        ref2 = res.reference()
        res2 = yield self.reg.get_resource_description(ref2)
        self.failUnless(res == res2)
        self.assertEqual(res1.name, 'foo')

        # Get the original
        res1 = yield self.reg.get_resource_description(ref1)
        self.assertEqual(res1.name, 'foo')
        
    def test_register_select_ancestor(self):
        raise unittest.SkipTest('Not implimented yet!')


    def test_set_lcastate(self):
        res = dataobject.ResourceDescription.create_new_resource()
        res.name = 'foo'
        res = yield self.reg.register_resource_description(res)
        ref1 = res.reference()
        
        #set by resource (Includes reference):
        ref2 = yield self.reg.set_resource_lcstate(res, dataobject.LCStates.active)
        
        # Used returend reference to set again:
        ref3 = yield self.reg.set_resource_lcstate(ref2, dataobject.LCStates.retired)
        
        res = yield self.reg.get_resource_description(ref3)
        self.assertEqual(res,dataobject.LCStates.retired)
        
        res = yield self.reg.get_resource_description(ref2)
        self.assertEqual(res,dataobject.LCStates.active)
        
        res = yield self.reg.get_resource_description(ref1)
        self.assertEqual(res,dataobject.LCStates.new)
        
        


    @defer.inlineCallbacks
    def test_registry_find(self):

        res1 = dataobject.ResourceDescription.create_new_resource()
        res1.name = 'foo'
        res1 = yield self.reg.register_resource_description(res1)
        
        res2 = dataobject.ResourceDescription.create_new_resource()
        res2.name = 'moo'
        res2 = yield self.reg.register_resource_description(res2)


        blank = dataobject.ResourceDescription.create_new_resource()
        results = yield self.reg.find_resource_description(blank,regex=False,ignore_defaults=False)
        self.assertEqual(results,[])
        
        results = yield self.reg.find_resource_description(res1,regex=False,ignore_defaults=False)
        self.assertIn(res1, results)
        self.assertNotIn(res2, results)

class RegistryCassandraTest(RegistryTest):
    """
    """

    def _set_up_backend(self):
        clist = ['amoeba.ucsd.edu:9160']
        ds = cassandra.CassandraStore.create_store(
            cass_host_list=clist,
            cf_super=True,            
            keyspace='Datastore',
            colfamily='DS1'
            )
        return ds





