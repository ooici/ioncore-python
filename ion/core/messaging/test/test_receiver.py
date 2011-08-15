#!/usr/bin/env python

"""
@file ion/play/test/test_hello_object.py
@test ion.play.hello_object Example unit tests for sample code.
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.test.iontest import IonTestCase

from ion.core.process.process import Process
from ion.core.messaging.receiver_test_service import ReceiverService, ReceiverServiceClient
from ion.core import bootstrap
from twisted.trial import unittest

from ion.core.object import object_utils

ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)

class ReceiverTest(IonTestCase):
    """
    Test sending to multiple receivers...
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        self.proc = Process()
        self.rsc = ReceiverServiceClient(proc=self.proc)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_op_a(self):

        rs = ReceiverService(spawnargs={'proc-name':'ReceiverService'})
        yield rs.spawn()

        msg = yield self.proc.message_client.create_instance(PERSON_TYPE)
        msg.name = 'David'

        fed = self.rsc.a(msg)

        context = yield rs.action

        resp, heads, message = yield fed

        self.assertEqual(context,heads['conv-id'])

        self.assertEqual(resp.name,'David')

    @defer.inlineCallbacks
    def test_op_b(self):

        rs = ReceiverService(spawnargs={'proc-name':'ReceiverService'})
        yield rs.spawn()

        msg = yield self.proc.message_client.create_instance(PERSON_TYPE)
        msg.name = 'David'

        fed = self.rsc.b(msg)

        context = yield rs.action

        resp, heads, message = yield fed

        self.assertEqual(context,heads['conv-id'])

        self.assertEqual(resp.name,'David')



    @defer.inlineCallbacks
    def test_op_a_and_b(self):

        rs_1 = ReceiverService(spawnargs={'proc-name':'ReceiverService1'})
        rs_2 = ReceiverService(spawnargs={'proc-name':'ReceiverService2'})
        yield rs_1.spawn()
        yield rs_2.spawn()

        msg = yield self.proc.message_client.create_instance(PERSON_TYPE)
        msg.name = 'David'

        a_fed = self.rsc.a(msg)
        b_fed = self.rsc.b(msg)

        context_1 = yield rs_1.action
        context_2 = yield rs_2.action


        a_resp, a_heads, a_message = yield a_fed
        b_resp, b_heads, b_message = yield b_fed

        self.failUnless(context_1 == a_heads['conv-id'] and context_2 == b_heads['conv-id'] or
            context_1 == b_heads['conv-id'] and context_2 == a_heads['conv-id'])
        self.assertNotEqual(context_1,context_2)

        self.assertEqual(a_resp.name,'David')
        self.assertEqual(b_resp.name,'David')


        b_fed = self.rsc.b(msg)
        a_fed = self.rsc.a(msg)

        context_1 = yield rs_1.action
        context_2 = yield rs_2.action


        a_resp, a_heads, a_message = yield a_fed
        b_resp, b_heads, b_message = yield b_fed

        self.failUnless(context_1 == a_heads['conv-id'] and context_2 == b_heads['conv-id'] or
            context_1 == b_heads['conv-id'] and context_2 == a_heads['conv-id'])
        self.assertNotEqual(context_1,context_2)

        self.assertEqual(a_resp.name,'David')
        self.assertEqual(b_resp.name,'David')