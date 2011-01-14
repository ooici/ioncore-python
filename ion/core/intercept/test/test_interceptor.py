#!/usr/bin/env python

"""
@file ion/core/intercept/test/test_interceptor.py
@author Michael Meisinger
@brief test interceptor system
"""
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.cc.container import Container
from ion.core.exception import ConfigurationError
from ion.core.intercept.interceptor import Interceptor, EnvelopeInterceptor
from ion.core.intercept.interceptor import PassThroughInterceptor, DropInterceptor
from ion.core.intercept.interceptor import Invocation
from ion.core.intercept.interceptor_system import InterceptorSystem
from ion.test.iontest import IonTestCase
from ion.util.config import Config

# Configuration
CONF = ioninit.config("ion.core.cc.container")
is_file = CONF.getValue('interceptor_system')
is_config = Config(ioninit.adjust_dir(is_file)).getObject()

class InterceptorTest(IonTestCase):
    """
    Testing interceptor system
    """

    #@defer.inlineCallbacks
    def setUp(self):
        #yield self._start_container()
        pass

    #@defer.inlineCallbacks
    def tearDown(self):
        #yield self._stop_container()
        pass

    @defer.inlineCallbacks
    def test_intercept(self):
        is_config1 = {
            'interceptors':{
                'pass':{
                    'classname':'ion.core.intercept.interceptor.PassThroughInterceptor'
                },
                'drop':{
                    'classname':'ion.core.intercept.interceptor.DropInterceptor'
                },
                'test1':{
                    'classname':'ion.core.intercept.test.test_interceptor.TestInterceptor',
                },
                'test2':{
                    'classname':'ion.core.intercept.test.test_interceptor.TestInterceptor',
                    'args':{'one':1, 'two':False},
                },
            },
            'stack':[
                {'name':'test1', 'interceptor':'test1' },
                {'name':'pass1', 'interceptor':'pass' },
                {'name':'test2', 'interceptor':'test2' },
            ]
        }

        intercept_sys = InterceptorSystem()
        yield intercept_sys.initialize(is_config1)
        yield intercept_sys.activate()

        intc = intercept_sys.interceptors
        self.assertEqual(len(intc), 4)
        self.assertIsInstance(intc['pass'],PassThroughInterceptor)
        self.assertIsInstance(intc['drop'],DropInterceptor)
        self.assertIsInstance(intc['test1'],TestInterceptor)
        self.assertIsInstance(intc['test2'],TestInterceptor)
        self.assertEqual(intc['test2'].arg1, 1)
        self.assertEqual(intc['test2'].arg2, False)

        paths = intercept_sys.paths
        self.assertEqual(len(paths), 2)
        self.assertEqual(len(paths[Invocation.PATH_IN]), 3)
        self.assertEqual(len(paths[Invocation.PATH_OUT]), 3)
        path_in = paths[Invocation.PATH_IN]
        self.assertEqual(path_in[0]['name'], 'test2')
        self.assertEqual(path_in[1]['name'], 'pass1')
        self.assertEqual(path_in[2]['name'], 'test1')
        ti1 = path_in[0]['interceptor_instance']
        ti2 = path_in[2]['interceptor_instance']
        self.assertIsInstance(ti1, TestInterceptor)
        self.assertIsInstance(ti2, TestInterceptor)

        inv1a = Invocation(path=Invocation.PATH_IN, message="123")
        inv1b = yield intercept_sys.process(inv1a)
        self.assertEqual(ti1.numbefore, 1)
        self.assertEqual(ti2.numbefore, 1)
        self.assertEqual(ti1.numafter, 0)
        self.assertEqual(ti2.numafter, 0)

        inv2a = Invocation(path=Invocation.PATH_OUT, message="123")
        inv2b = yield intercept_sys.process(inv2a)
        self.assertEqual(ti1.numbefore, 1)
        self.assertEqual(ti2.numbefore, 1)
        self.assertEqual(ti1.numafter, 1)
        self.assertEqual(ti2.numafter, 1)

    @defer.inlineCallbacks
    def test_intercept_drop(self):
        is_config1 = {
            'interceptors':{
                'drop1':{
                    'classname':'ion.core.intercept.interceptor.DropInterceptor'
                },
                'test1':{
                    'classname':'ion.core.intercept.test.test_interceptor.TestInterceptor',
                },
                'test3':{
                    'classname':'ion.core.intercept.test.test_interceptor.TestInterceptor',
                },
            },
            'stack':[
                {'name':'test1', 'interceptor':'test1' },
                {'name':'drop1', 'interceptor':'drop1' },
                {'name':'test3', 'interceptor':'test3' },
            ]
        }

        intercept_sys = InterceptorSystem()
        yield intercept_sys.initialize(is_config1)
        yield intercept_sys.activate()
        ti1 = intercept_sys.interceptors['test1']
        ti2 = intercept_sys.interceptors['test3']

        inv1a = Invocation(path=Invocation.PATH_IN, message="123")
        inv1b = yield intercept_sys.process(inv1a)
        self.assertEqual(ti1.numbefore, 0)
        self.assertEqual(ti2.numbefore, 1)
        self.assertEqual(ti1.numafter, 0)
        self.assertEqual(ti2.numafter, 0)

        inv2a = Invocation(path=Invocation.PATH_OUT, message="123")
        inv2b = yield intercept_sys.process(inv2a)
        self.assertEqual(ti1.numbefore, 0)
        self.assertEqual(ti2.numbefore, 1)
        self.assertEqual(ti1.numafter, 1)
        self.assertEqual(ti2.numafter, 0)

    @defer.inlineCallbacks
    def test_intercept_fail(self):
        is_config1 = {}
        try:
            intercept_sys = InterceptorSystem()
            yield intercept_sys.initialize(is_config1)
            self.fail("ConfigurationError expected")
        except ConfigurationError, ce:
            pass

        is_config2 = {
            'interceptors':{
                'test1':{
                    'classname':'ion.core.$$NON-exis ting',
                },
            },
        }
        try:
            intercept_sys = InterceptorSystem()
            yield intercept_sys.initialize(is_config2)
            self.fail("ConfigurationError expected")
        except AttributeError, ce:
            pass

        is_config3 = {
            'interceptors':{
                'test1':{
                    'classname':'ion.core.intercept.test.test_interceptor.TestInterceptor',
                },
            },
            'stack':[
                {'name':'test1', 'interceptor':'test1' },
                {'name':'test2', 'interceptor':'test2' },
            ]
        }
        try:
            intercept_sys = InterceptorSystem()
            yield intercept_sys.initialize(is_config3)
            self.fail("ConfigurationError expected")
        except ConfigurationError, ce:
            pass

        is_config4 = {
            'interceptors':{
                'test1':{
                    'classname':'ion.core.intercept.test.test_interceptor.TestInterceptor',
                },
            },
            'stack':[
                {'name':'test1'},
            ]
        }
        try:
            intercept_sys = InterceptorSystem()
            yield intercept_sys.initialize(is_config4)
            self.fail("ConfigurationError expected")
        except ConfigurationError, ce:
            pass

    @defer.inlineCallbacks
    def test_intercept_container(self):
        yield self._start_container()
        try:
            intercept_sys = ioninit.container_instance.interceptor_system
            self.assertIsInstance(intercept_sys, InterceptorSystem)
            self.assertEqual(intercept_sys._get_state(),"ACTIVE")
            self.assertEqual(len(intercept_sys.interceptors), len(is_config['interceptors']))
        finally:
            yield self._stop_container()

class TestInterceptor(EnvelopeInterceptor):
    """
    Interceptor to test messages.
    """
    def on_initialize(self, *args, **kwargs):
        if self.name == 'test2':
            self.arg1 = kwargs['one']
            self.arg2 = kwargs['two']
        self.numbefore = 0
        self.numafter = 0

    def before(self, invocation):
        log.debug("interceptor %s IN" % self.name)
        self.numbefore += 1
        invocation.proceed()
        return invocation

    def after(self, invocation):
        log.debug("interceptor %s OUT" % self.name)
        self.numafter += 1
        invocation.proceed()
        return invocation


class TestSignature(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        is_config_sig = {
            'interceptors':{
                'signature':{
                    'classname':'ion.core.intercept.signature.SystemSecurityPlugin'
                    },
                },
            'stack':[
                {'name':'signature', 'interceptor':'signature'},
                ]
            }

        intercept_sys = InterceptorSystem()
        yield intercept_sys.initialize(is_config_sig)
        yield intercept_sys.activate()
        self.intercept_sys = intercept_sys


    @defer.inlineCallbacks
    def test_signature_headers(self):
        """
        Test that signature headers are set when message passes through
        outgoing interceptor stack with signature interceptor

        """
        msg = {'content':'foo'}
        inv_pre = Invocation(path=Invocation.PATH_OUT, message=msg)
        inv_post = yield self.intercept_sys.process(inv_pre)
        message = inv_post.message

        self.failUnless(message.has_key('signature'))
        self.failUnlessEqual(message['signer'], 'ooi-ion') #XXX Hard-coded assumption! 


    @defer.inlineCallbacks
    def test_allow(self):
        """
        Test case where a message is allowed in and out of the signature
        interceptor
        """
        msg = {'content':'foo'}
        inv_outgoing_a = Invocation(path=Invocation.PATH_OUT, message=msg)
        inv_outgoing_b = yield self.intercept_sys.process(inv_outgoing_a)

        self.failUnlessEqual(inv_outgoing_b.status,
                Invocation.STATUS_PROCESS)

        inv_incoming_a = inv_outgoing_b #use the last outgoing as the
                                        #incoming so the headers are already set
        inv_incoming_a.path = Invocation.PATH_IN #set to incoming

        inv_incoming_b = yield self.intercept_sys.process(inv_incoming_a)

        self.failUnlessEqual(inv_incoming_b.status, 
                Invocation.STATUS_PROCESS)


    @defer.inlineCallbacks
    def test_drop(self):
        msg = {'content':'foo'}
        inv_outgoing_a = Invocation(path=Invocation.PATH_OUT, message=msg)
        inv_outgoing_b = yield self.intercept_sys.process(inv_outgoing_a)

        self.failUnlessEqual(inv_outgoing_b.status,
                Invocation.STATUS_PROCESS)

        inv_incoming_a = inv_outgoing_b #use the last outgoing as the
                                        #incoming so the headers are already set
        inv_incoming_a.path = Invocation.PATH_IN #set to incoming
        inv_incoming_a.message['content'] = 'bar' #invalidate message

        inv_incoming_b = yield self.intercept_sys.process(inv_incoming_a)

        self.failUnlessEqual(inv_incoming_b.status, 
                Invocation.STATUS_DROP)







