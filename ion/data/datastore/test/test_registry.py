#!/usr/bin/env python

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest

from ion.data import store
from ion.data.backends import store_service
from ion.data.backends import cassandra
from ion.data.datastore import registry
from ion.data import dataobject


from ion.test.iontest import IonTestCase
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

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

        res = dataobject.Resource.create_new_resource()
        res.name = 'foo'

        #print 'Dataobject Created',dataobject.DataObject._types.has_key('__builtins__')

        #@note - always over-write the old argument value!
        res = yield self.reg.register_resource(res)

        #print 'Dataobject Registerd!',dataobject.DataObject._types.has_key('__builtins__')

        #res.reference is a DataObject function. DataObject can't specify
        #functions, only data; this does not work with with DEncoder and
        #would not work with a object description language
        ref = res.reference()
        res2 = yield self.reg.get_resource(ref)
        self.failUnless(res == res2)



    @defer.inlineCallbacks
    def test_register_overwrite(self):
        res = dataobject.Resource.create_new_resource()
        res.name = 'foo'
        res = yield self.reg.register_resource(res)
        ref1 = res.reference()
        # get this version back again
        res1 = yield self.reg.get_resource(ref1)
        self.failUnless(res == res1)
        self.assertEqual(res1.name, 'foo')

        #update with new data
        res.name = 'moo'
        res = yield self.reg.register_resource(res)

        # get the new version back again
        ref2 = res.reference()
        res2 = yield self.reg.get_resource(ref2)
        self.failUnless(res == res2)
        self.assertEqual(res2.name, 'moo')

        # Get the original
        res1 = yield self.reg.get_resource(ref1)
        self.assertEqual(res1.name, 'foo')

    def test_register_select_ancestor(self):
        raise unittest.SkipTest('Not implimented yet!')


    def test_set_lcstate(self):
        res = dataobject.Resource.create_new_resource()
        res.name = 'foo'
        res = yield self.reg.register_resource(res)
        ref1 = res.reference()

        #set by resource (Includes reference):
        ref2 = yield self.reg.set_resource_lcstate(res, dataobject.LCStates.active)

        # Used returend reference to set again:
        ref3 = yield self.reg.set_resource_lcstate(ref2, dataobject.LCStates.retired)

        ref4 = yield self.reg.set_resource_lcstate_commissioned(ref3)

        res = yield self.reg.get_resource(ref4)
        self.assertEqual(res.lifecycle,dataobject.LCStates.retired)

        res = yield self.reg.get_resource(ref3)
        self.assertEqual(res.lifecycle,dataobject.LCStates.retired)

        res = yield self.reg.get_resource(ref2)
        self.assertEqual(res.lifecycle,dataobject.LCStates.active)

        res = yield self.reg.get_resource(ref1)
        self.assertEqual(res.lifecycle,dataobject.LCStates.new)




    @defer.inlineCallbacks
    def test_registry_find(self):

        res1 = dataobject.Resource.create_new_resource()
        res1.name = 'foo'
        res1 = yield self.reg.register_resource(res1)

        res2 = dataobject.Resource.create_new_resource()
        res2.name = 'moo'
        res2 = yield self.reg.register_resource(res2)


        blank = dataobject.Resource()

        results = yield self.reg.find_resource(blank,regex=False,ignore_defaults=False)
        self.assertEqual(results,[])

        results = yield self.reg.find_resource(res1,regex=False,ignore_defaults=False)
        self.assertIn(res1, results)
        self.assertNotIn(res2, results)

        results = yield self.reg.find_resource(blank,regex=False,ignore_defaults=True)
        self.assertIn(res1, results)
        self.assertIn(res2, results)

        results = yield self.reg.find_resource(blank,regex=True,ignore_defaults=True)
        self.assertIn(res1, results)
        self.assertIn(res2, results)

        blank.name='oo'
        results = yield self.reg.find_resource(blank,regex=True,ignore_defaults=True)
        self.assertIn(res1, results)
        self.assertIn(res2, results)

        blank.name='mo'
        results = yield self.reg.find_resource(blank,regex=True,ignore_defaults=True)
        self.assertNotIn(res1, results)
        self.assertIn(res2, results)

        blank.name='mo'
        results = yield self.reg.find_resource(blank,regex=True,attnames=['name',])
        self.assertNotIn(res1, results)
        self.assertIn(res2, results)

        blank.name='moo'
        results = yield self.reg.find_resource(blank,attnames=['name',])
        self.assertNotIn(res1, results)
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

        services = [
            {'name':'registry1','module':'ion.data.datastore.registry','class':'RegistryService'},
        ]

        #description_utility.load_descriptions()
        sup = yield self._spawn_processes(services)
        self.reg = registry.RegistryClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.reg.clear_registry()
        yield self._stop_container()
