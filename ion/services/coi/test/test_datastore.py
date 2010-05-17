#!/usr/bin/env python

"""
@file ion/services/coi/test/test_datastore.py
@author Michael Meisinger
@brief test datastore service
"""

import logging
from twisted.internet import defer
from twisted.trial import unittest

from ion.core import base_process
from ion.data.objstore import ValueObject, TreeValue, CommitValue, RefValue, ValueRef
from ion.data.objstore import ObjectStore, ValueStore
from ion.services.coi.datastore import DatastoreService, DatastoreClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class DatastoreSvcMockTest(unittest.TestCase):
    """
    Testing client class of object store
    """
    def test_Datastore(self):
        oss = DatastoreService()
        oss.receiver = pu.FakeReceiver()
        oss.slc_init()
        msg = pu.FakeMessage({'reply-to':'fake'})

        vo1 = ValueObject('1')
        vo2 = ValueObject('2')
        vo3 = ValueObject('3')
        vo4 = ValueObject('4')
        vo5 = ValueObject('5')

        tv1 = TreeValue((vo1, vo2))
        tv2 = TreeValue((vo3, vo4))
        tv3 = TreeValue(vo5)
        tv4 = TreeValue(tv1)

        def _cont(key, value=None, children=None, parents=None):
            res = {}
            if type(key) is list: res['keys'] = key
            else: res['key'] = key
            if value: res['value'] = value
            if children: res['children'] = children
            if parents: res['parents'] = parents
            return res

        def _opfix(op, cont):
            """Calls a service op with receive params and gets result in
            reply from fake receiver"""
            getattr(oss, 'op_'+op)(cont, {'sender':'procid.1'}, msg)
            return oss.receiver.sendmsg['content']

        r1 = _opfix('put',_cont('key1','1'))
        print "r1=", r1
        r2 = _opfix('put',_cont('key2','2'))
        r3 = _opfix('put',_cont('key3','3'))
        r4 = _opfix('put',_cont('key4','4'))
        r5 = _opfix('put',_cont('key5','5'))

        r11 = _opfix('put',_cont('key11','11',None,r1))
        r12 = _opfix('put',_cont('key12','12',None,r11))
        r13 = _opfix('put',_cont('key13','13'))
        r14 = _opfix('put',_cont('key14','14',None,(r12,r13)))
        r15 = _opfix('put',_cont('key15','15',None,r14))

        # Check get_values
        rv1 = _opfix('get_values',_cont(['key1','key2','key3','key4']))
        print "rv1=", rv1
        self.assertEqual(len(rv1),4)
        self.assertEqual(vo1.identity,rv1[0]['identity'])
        self.assertEqual(vo2.identity,rv1[1]['identity'])
        self.assertEqual(vo3.identity,rv1[2]['identity'])
        self.assertEqual(vo4.identity,rv1[3]['identity'])
        
        # Check get_ancestors
        ra1 = _opfix('get_ancestors',_cont('key1'))
        print "ra1=", ra1
        self.assertEqual(len(ra1),0)
        
        ra15 = _opfix('get_ancestors',_cont('key15'))
        print "ra15=", ra15
        self.assertEqual(len(ra15),5)

class DatastoreServiceTest(IonTestCase):
    """
    Testing service classes of data store
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_put(self):
        services = [
            {'name':'datastore1','module':'ion.services.coi.datastore','class':'DatastoreService'},
            #{'name':'objstoreSvc2','module':'ion.data.objstore','class':'ObjectStoreService'},
        ]

        sup = yield self._spawn_processes(services)

        osc = DatastoreClient(sup)

        res1 = yield osc.put('key1','value1')
        logging.info('Result1 put: '+str(res1))

        res2 = yield osc.get('key1')
        logging.info('Result2 get: '+str(res2))

        res3 = yield osc.put('key1','value2')
        logging.info('Result3 put: '+str(res3))

        res4 = yield osc.put('key2','value1')
        logging.info('Result4 put: '+str(res4))
