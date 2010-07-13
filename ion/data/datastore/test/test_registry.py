
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

from ion.resources.coi_resource_descriptions import ComplexResource

class RegistryTest(unittest.TestCase):
    """
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._setup_backend()
        
    @defer.inlineCallbacks
    def _setup_backend(self):
        s = yield store.Store.create_store()
        self.reg = registry.Registry(s)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.reg.clear_registry()


    @defer.inlineCallbacks
    def test_register(self):
        res = dataobject.ResourceDescription.create_new_resource()
        #res = ComplexResource.create_new_resource()
        res.name = 'foo'
        #@Note - always over-write the old argument value!
        res = yield self.reg.register_resource_description(res)
        
        print 'Saved Resource:',res
        print 'Resource Type:',type(res)
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


        blank = dataobject.ResourceDescription()
        results = yield self.reg.find_resource_description(blank,regex=False,ignore_defaults=False)
        self.assertEqual(results,[])
        
        results = yield self.reg.find_resource_description(res1,regex=False,ignore_defaults=False)
        self.assertIn(res1, results)
        self.assertNotIn(res2, results)
        
        results = yield self.reg.find_resource_description(blank,regex=False,ignore_defaults=True)
        self.assertIn(res1, results)
        self.assertIn(res2, results)
        

class RegistryCassandraTest(RegistryTest):
    """
    """

    @defer.inlineCallbacks
    def _setup_backend(self):
        clist = ['amoeba.ucsd.edu:9160']
        s = yield cassandra.CassandraStore.create_store(
            cass_host_list=clist,
            cf_super=True,            
            keyspace='Datastore',
            colfamily='DS1'
            )
        self.reg = registry.Registry(s)


class RegistryServiceTest(IonTestCase, RegistryTest):
    """
    """
    @defer.inlineCallbacks
    def _setup_backend(self):
        yield self._start_container()
        # By default, the store service will use Store in the backend.
        services = [
            {'name':'registry1','module':'ion.data.datastore.registry','class':'BaseRegistryService'},
        ]
        
        print 'Dataobject._types',dataobject.DataObject._types
        
        sup = yield self._spawn_processes(services)
        #print 'Dataobject._types',dataobject.DataObject._types
        self.reg = registry.BaseRegistryClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.reg.clear_registry()
        yield self._stop_container()



class TestLoadTypes(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        print 'Dataobject._types',dataobject.DataObject._types
        
    def test(self):
        print 'testing'


