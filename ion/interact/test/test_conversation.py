#!/usr/bin/env python

"""
@file ion/tnteract/test/test_conversations.py
@author Michael Meisinger
@brief test case for conversations
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.process.process import Process, ProcessDesc, ProcessFactory
from ion.core.cc.container import Container
from ion.core.exception import ReceivedError, ConversationError
from ion.interact.conversation import Conversation, ConversationType, conv_mgr_instance
from ion.interact.request import RequestType, Request
from ion.interact.rpc import RpcType, Rpc
from ion.test.iontest import IonTestCase, ReceiverProcess
import ion.util.procutils as pu

class ConversationTest(IonTestCase):
    """
    Tests the conversation management.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_basic_conversations(self):
        conv_mgr = conv_mgr_instance
        self.assertTrue(conv_mgr)

        ctypes = conv_mgr.conv_type_registry
        self.assertTrue(type(ctypes) is dict)
        self.assertTrue(len(ctypes) >= 2)

        req_type = conv_mgr.get_conversation_type(RequestType.CONV_TYPE_REQUEST)
        self.assertIsInstance(req_type, ConversationType)

        rpc_type = conv_mgr.get_conversation_type(RpcType.CONV_TYPE_RPC)
        self.assertIsInstance(rpc_type, ConversationType)

        try:
            other_type = conv_mgr.get_conversation_type("NON-EXISTING")
            self.fail("ConversationError expected for unregistered conv type")
        except ConversationError, ce:
            log.debug("Successfully caught unregistered conversation type")

        conv_id = conv_mgr.create_conversation_id()
        req_inst = req_type.new_conversation(conv_type=RequestType.CONV_TYPE_REQUEST, conv_id=conv_id)
        self.assertIsInstance(req_inst, Conversation)
        self.assertIsInstance(req_inst, Request)

    @defer.inlineCallbacks
    def test_process_conversations(self):
        proc1 = Process()
        pid1 = yield proc1.spawn()

        proc2 = Process()
        pid2 = yield proc2.spawn()

        conv_mgr = proc1.conv_manager
        self.assertTrue(conv_mgr)

        req_conv = conv_mgr.new_conversation(RequestType.CONV_TYPE_REQUEST)
        req_conv.bind_role_local(RequestType.ROLE_INITIATOR.role_id, proc1)
        req_conv.bind_role(RequestType.ROLE_PARTICIPANT.role_id, pid2)
