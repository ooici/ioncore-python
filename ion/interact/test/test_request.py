#!/usr/bin/env python

"""
@file ion/tnteract/test/test_request.py
@author Michael Meisinger
@brief test case for request conversations
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.process.process import Process, ProcessDesc, ProcessFactory
from ion.core.cc.container import Container
from ion.core.exception import ReceivedError
from ion.interact.conversation import Conversation
from ion.test.iontest import IonTestCase, ReceiverProcess
import ion.util.procutils as pu

class RequestConvTest(IonTestCase):
    """
    Tests the request conversation.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_request_process(self):
        processes = [
            {'name':'req1','module':'ion.interact.test.test_request','class':'ParticipantProcess'},
            {'name':'req2','module':'ion.interact.test.test_request','class':'ParticipantProcess'},
        ]
        sup = yield self._spawn_processes(processes)

        req1pid = sup.get_child_id('req1')
        req1proc = self._get_procinstance(req1pid)

        req2pid = sup.get_child_id('req2')
        req2proc = self._get_procinstance(req2pid)

        req_content = {}

        print "-------------------- TEST BEGIN"
        res = yield req1proc.request(receiver=req2pid, action="action1", content="my request")
        print "-------------------- TEST END"

        yield self._shutdown_processes()

class ParticipantProcess(Process):

    @defer.inlineCallbacks
    def op_action1(self, content, headers, msg):
        log.debug("Participant process action called. Sending reply")
        reply_content = "OK"
        yield self.reply_ok(msg, reply_content)

# Spawn of the process using the module name
factory = ProcessFactory(ParticipantProcess)
