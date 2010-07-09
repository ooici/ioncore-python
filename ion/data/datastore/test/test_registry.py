
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
        self.reg = registry.ResourceRegistry(s)
        self.mystore = s

    
    def _set_up_backend(self):
        s = store.Store.create_store()
        return (s)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.mystore.clear_store()


    @defer.inlineCallbacks
    def test_register(self):
        res = registry.ResourceDescription.create_new_resource()
        res.name = 'foo'
        res = yield self.reg.register(res)
        
        ref = res.reference()
        res2 = yield self.reg.get_description(ref)
        #print res
        #print res2
        self.failUnless(res == res2)

    def test_lcstate(self):
        
        #logging.info(registry.LCStates)
        
        res = registry.Generic.create_new_resource()
        logging.info(res.get_lifecyclestate())
        
        res.set_lifecyclestate(registry.LCStates.active)
        logging.info(res.get_lifecyclestate())
        res.set_lifecyclestate(registry.LCStates['retired'])
        logging.info(res.get_lifecyclestate())



    @defer.inlineCallbacks
    def test_register_overwrite(self):
        res = registry.ResourceDescription.create_new_resource()
        res.name = 'foo'
        ref = yield self.reg.register(res)
        #logging.info(str(res))
        #logging.info(str(ref))
        res.name = 'moo'
        ref = yield self.reg.register(res)

        res2 = yield self.reg.get_description(ref)
        self.failUnless(res == res2)

    @defer.inlineCallbacks
    def test_register_get_list(self):
        res1 = registry.ResourceDescription.create_new_resource()
        res1.name = 'foo'
        res1 = yield self.reg.register(res1)

        res2 = registry.ResourceDescription.create_new_resource()
        res2.name = 'moo'
        res2 = yield self.reg.register(res2)

        ref_list = yield self.reg.list()
        #print res_list
        
        # Can't compare the resource description to the reference list
        #self.assertIn(res1.reference(), ref_list)
        
        res_s = yield self.reg.list_descriptions()
        self.assertEqual(len(res_s), 2)
            
            
        self.assertIn(res1, res_s)        
        self.assertIn(res2, res_s)        


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





