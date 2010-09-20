#!/usr/bin/env python

"""
@file ion/services/coi/test/test_attributestore.py
@author Michael Meisinger
@brief test attribute store service
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.core import base_process
from ion.services.coi.attributestore import AttributeStoreService, AttributeStoreClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu


class AttrStoreServiceTest(IonTestCase):
    """
    Testing service classes of attribute store
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_put_seperate_backend(self):
        # Test with seperate store backends
        services = [
            {'name':'attstore1',
            'module':'ion.services.coi.attributestore',
            'class':'AttributeStoreService',
            'spawnargs':{
                'servicename':'as1',
                'backend_class':'ion.data.store.Store',
                'backend_args':{}
                    }
                },
            {'name':'attstore2',
            'module':'ion.services.coi.attributestore',
            'class':'AttributeStoreService',
            'spawnargs':{
                'servicename':'as2',
                'backend_class':'ion.data.store.Store',
                'backend_args':{}
                    }
                },
        ]

        sup = yield self._spawn_processes(services)

        asc1 = AttributeStoreClient(proc=sup, targetname='as1')

        res1 = yield asc1.put('key1','value1')
        log.info('Result1 put: '+str(res1))

        res2 = yield asc1.get('key1')
        log.info('Result2 get: '+str(res2))
        self.assertEqual(res2, 'value1')

        res3 = yield asc1.put('key1','value2')

        res4 = yield asc1.get('key1')
        self.assertEqual(res4, 'value2')

        res5 = yield asc1.get('non_existing')
        self.assertEqual(res5, None)

        asc2 = AttributeStoreClient(proc=sup, targetname='as2')

        # With separate backends this should return none
        resx1 = yield asc2.get('key1')
        self.assertEqual(resx1, None)

        yield asc1.clear_store()
        yield asc2.clear_store()


    @defer.inlineCallbacks
    def test_put_common_backend(self):
        # Test with cassandra store backend where both services can access common values!
        services = [
            {'name':'Junk1',
             'module':'ion.services.coi.attributestore',
             'class':'AttributeStoreService',
             'spawnargs':{'servicename':'as1', # this is the name of the instance!
                            'backend_class':'ion.data.backends.cassandra.CassandraStore',
                            'backend_args':{'cass_host_list':['amoeba.ucsd.edu:9160'],
                                        'keyspace':'Datastore',
                                        'colfamily':'DS1',
                                        'cf_super':True,
                                        'namespace':'ours',
                                        'key':'Junk'
                                        }}},
            {'name':'Junk2',
            'module':'ion.services.coi.attributestore',
            'class':'AttributeStoreService',
            'spawnargs':{'servicename':'as2', # this is the name of the instance!
                        'backend_class':'ion.data.backends.cassandra.CassandraStore',
                        'backend_args':{'cass_host_list':['amoeba.ucsd.edu:9160'],
                                        'keyspace':'Datastore',
                                        'colfamily':'DS1',
                                        'cf_super':True,
                                        'namespace':'ours',
                                        'key':'Junk'
                                        }}}
                    ]

        sup = yield self._spawn_processes(services)

        asc1 = AttributeStoreClient(proc=sup, targetname='as1')

        res1 = yield asc1.put('key1','value1')
        log.info('Result1 put: '+str(res1))

        res2 = yield asc1.get('key1')
        log.info('Result2 get: '+str(res2))
        self.assertEqual(res2, 'value1')

        res3 = yield asc1.put('key1','value2')

        res4 = yield asc1.get('key1')
        self.assertEqual(res4, 'value2')

        res5 = yield asc1.get('non_existing')
        self.assertEqual(res5, None)

        asc2 = AttributeStoreClient(proc=sup, targetname='as2')

        tres1 = yield asc2.put('tkey1','tvalue1')
        log.info('tResult1 put: '+str(tres1))

        tres2 = yield asc2.get('tkey1')
        log.info('tResult2 get: '+str(tres2))
        self.assertEqual(tres2, 'tvalue1')

        # Let cassandra register the new entry
        pu.asleep(5)

        # With common backends the value should be found.
        resx1 = yield asc2.get('key1')
        self.assertEqual(resx1, 'value2',msg='Failed to pull value from second service instance')

        yield asc1.clear_store()
        yield asc2.clear_store()
