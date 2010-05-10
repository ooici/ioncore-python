#!/usr/bin/env python

"""
@file ion/data/test_objstore.py
@author Michael Meisinger
@brief test object store
"""

import logging
from twisted.internet import defer
from twisted.trial import unittest

from ion.core import base_process, bootstrap
from ion.data.objstore import ObjectStoreClient, ObjectStoreService, ValueObject
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class ObjectStoreTest(unittest.TestCase):
    """Testing client classes of object store
    """

    def test_ValueObjects(self):
        vo1 = ValueObject('1')
        self.assertNotEqual(vo1.identity, None)
        # Basic value objects
        print vo1
        print "vo1=", vo1.__dict__

        vo2 = ValueObject(2)
        self.assertNotEqual(vo1.identity, vo2.identity)

        vo3 = ValueObject(('a','b'))
        self.assertNotEqual(vo1.identity, vo3.identity)
        vo4 = ValueObject(['a','b'])
        vo5 = ValueObject({'a':'b', 'c':(1,2), 'd':{}, 'e':{'x':'y'}})
        self.assertNotEqual(vo5.identity, vo4.identity)
        print "vo5=", vo5.__dict__

        # Composite value objects with childrefs
        voc0 = ValueObject('comp1',None)
        voc1 = ValueObject('comp1',())
        self.assertEqual(voc0.identity, voc1.identity)

        voc2 = ValueObject('comp1',vo1.identity)
        voc3 = ValueObject('comp1',(vo1.identity,))
        voc4 = ValueObject('comp1',vo1)
        voc5 = ValueObject('comp1',(vo1,))
        self.assertEqual(voc2.identity, voc3.identity)
        self.assertEqual(voc4.identity, voc5.identity)
        self.assertEqual(voc2.identity, voc4.identity)
        self.assertNotEqual(voc0.identity, voc2.identity)

        voc6 = ValueObject('comp1',(vo2,vo3))
        print "voc6=", voc6.__dict__
        voc7 = ValueObject('comp1',voc6)
        voc8 = ValueObject('comp1',(vo3,vo2))
        self.assertNotEqual(voc6.identity, voc8.identity)

        # Composite value objects with base
        vob0 = ValueObject('comp1',None,None)
        vob1 = ValueObject('comp1',None,(vo2,))
        vob2 = ValueObject('comp1',None,vo2)
        vob3 = ValueObject('comp1',None,(vo2.identity,))
        vob4 = ValueObject('comp1',None,vo2.identity)
        self.assertEqual(vob1.identity, vob2.identity)
        self.assertEqual(vob3.identity, vob4.identity)
        self.assertEqual(vob1.identity, vob3.identity)
        self.assertNotEqual(vob0.identity, vob1.identity)

        vob5 = ValueObject('comp1',None,(vo1,vob0))
        vob6 = ValueObject('comp1',None,vob5)
        vob7 = ValueObject('comp1',None,(vob0,vo1))
        self.assertNotEqual(vob5.identity, vob7.identity)

        # Composite value objects with childref and base
        vobc1 = ValueObject('comp1',(vo1,vo2),(vob1,vob2))
        print "vobc1=", vobc1.__dict__

    def test_ObjectStore(self):
        oss = ObjectStoreService()
        oss.receiver = pu.FakeReceiver()
        oss.slc_init()
        msg = pu.FakeMessage({'reply-to':'fake'})

        vo1 = ValueObject('1')
        vo2 = ValueObject('2')
        vo3 = ValueObject('3',(vo1,vo2))
        vo4 = ValueObject('4')
        vo5 = ValueObject('5',(vo3,vo4))

        vo11 = ValueObject('11',None,vo5)
        vo12 = ValueObject('12',None,vo11)
        vo13 = ValueObject('13')
        vo14 = ValueObject('14',None,(vo12,vo13))
        vo15 = ValueObject('15',None,vo14)

        def _cont(key, value=None, childrefs=None, basedon=None):
            res = {}
            if type(key) is list: res['keys'] = key
            else: res['key'] = key
            if value: res['value'] = value
            if childrefs: res['childrefs'] = childrefs
            if basedon: res['basedon'] = basedon
            return res

        def _opfix(op, cont):
            """Calls a service op with receive params and gets result in
            reply from fake receiver"""
            getattr(oss, 'op_'+op)(cont, {}, msg)
            return oss.receiver.sendmsg['content']

        r1 = _opfix('put',_cont('key1','1'))
        print "r1=", r1
        r2 = _opfix('put',_cont('key2','2'))
        r3 = _opfix('put',_cont('key3','3',(r1['identity'],r2['identity'])))
        r4 = _opfix('put',_cont('key4','4'))
        r5 = _opfix('put',_cont('key5','5',(r3['identity'],r4['identity'])))

        r11 = _opfix('put',_cont('key11','11',None,r5['identity']))
        r12 = _opfix('put',_cont('key12','12',None,r11['identity']))
        r13 = _opfix('put',_cont('key13','13'))
        r14 = _opfix('put',_cont('key14','14',None,(r12['identity'],r13['identity'])))
        r15 = _opfix('put',_cont('key15','15',None,r14['identity']))

        r16 = _opfix('put',_cont('key16','1'))

        # Check get
        fg0 = _opfix('get',_cont('key0'))
        print "fg0=", fg0
        self.assertEqual(len(fg0),0)

        fg1 = _opfix('get',_cont('key1'))
        print "fg1=", fg1
        self.assertEqual(fg1['value'],'1')
        self.assertEqual(fg1['value'],vo1.state['value'])
        self.assertEqual(fg1['identity'],vo1.identity)

        fg2 = _opfix('get',_cont('key2'))
        self.assertEqual(fg2['identity'],vo2.identity)

        fg3 = _opfix('get',_cont('key3'))
        self.assertIn(vo1.identity,fg3['childrefs'])
        self.assertIn(vo2.identity,fg3['childrefs'])
        self.assertEqual(len(fg3['basedon']),0)
        print "vo3=",vo3.state
        self.assertEqual(fg3['identity'],vo3.identity)

        fg5 = _opfix('get',_cont('key5'))
        print "fg5=", fg5
        self.assertIn(vo3.identity,fg5['childrefs'])

        fg11 = _opfix('get',_cont('key11'))
        self.assertEqual(len(fg11['childrefs']),0)
        self.assertIn(vo5.identity,fg11['basedon'])

        fg14 = _opfix('get',_cont('key14'))
        self.assertEqual(len(fg14['basedon']),2)
        self.assertIn(vo12.identity,fg14['basedon'])
        self.assertIn(vo13.identity,fg14['basedon'])

        # Check get_values
        rv1 = _opfix('get_values',_cont([r1['identity'],r2['identity'],r4['identity'],r5['identity']]))
        print "rv1=", rv1
        self.assertEqual(len(rv1),4)
        self.assertEqual(r1['identity'],rv1[0]['identity'])
        self.assertEqual(r2['identity'],rv1[1]['identity'])
        self.assertEqual(r4['identity'],rv1[2]['identity'])
        self.assertEqual(r5['identity'],rv1[3]['identity'])

        # Check get_ancestors
        ra1 = _opfix('get_ancestors',_cont(r1['identity']))
        print "ra1=", ra1
        self.assertEqual(len(ra1),0)

        ra5 = _opfix('get_ancestors',_cont(r5['identity']))
        print "ra5=", ra5
        self.assertEqual(len(ra5),0)

        ra15 = _opfix('get_ancestors',_cont(r15['identity']))
        print "ra15=", ra15
        self.assertEqual(len(ra15),5)

class ObjectStoreServiceTest(IonTestCase):
    """Testing service classes of object store
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._startContainer()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stopContainer()


    @defer.inlineCallbacks
    def test_put(self):
        services = [
            {'name':'objstoreSvc1','module':'ion.data.objstore','class':'ObjectStoreService'},
            #{'name':'objstoreSvc2','module':'ion.data.objstore','class':'ObjectStoreService'},
        ]

        yield bootstrap.bootstrap(None, services)

        sup = yield base_process.procRegistry.get("bootstrap")
        logging.info("Supervisor: "+repr(sup))

        oss1 = yield base_process.procRegistry.get("objstoreSvc1")
        osc = ObjectStoreClient(oss1)
        yield osc.attach()

        res1 = yield osc.put('key1','value1')
        logging.info('Result1 put: '+str(res1))

        res2 = yield osc.get('key1')
        logging.info('Result2 get: '+str(res2))

        res3 = yield osc.put('key1','value2')
        logging.info('Result3 put: '+str(res3))

        res4 = yield osc.put('key2','value1')
        logging.info('Result4 put: '+str(res4))
