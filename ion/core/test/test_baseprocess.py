#!/usr/bin/env python

"""
@file ion/core/test/test_baseprocess.py
@author Michael Meisinger
@brief test case for process base class
"""

import os
import sha
import logging
logging = logging.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn

from ion.core import ioninit
from ion.core.base_process import BaseProcess, ProcessDesc, ProtocolFactory

from ion.test.iontest import IonTestCase, ReceiverProcess
import ion.util.procutils as pu

class BaseProcessTest(IonTestCase):
    """
    Tests the process base classe.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_process_basics(self):
        p1 = BaseProcess()
        self.assertTrue(p1.receiver)

        rec = Receiver("myname")
        p2 = BaseProcess(rec)

        args = {'arg1':'value1','arg2':{}}
        p3 = BaseProcess(None, args)
        self.assertEquals(p3.spawn_args, args)

        pid1 = yield p1.spawn()


    @defer.inlineCallbacks
    def test_child_processes(self):
        p1 = BaseProcess()
        pid1 = yield p1.spawn()
        yield p1.init()

        child = ProcessDesc(name='echo', module='ion.core.test.test_baseprocess')
        pid2 = yield p1.spawn_child(child)

        (cont,hdrs,msg) = yield p1.rpc_send(pid2,'echo','content123')
        self.assertEquals(cont['value'], 'content123')

        yield p1.shutdown()

    @defer.inlineCallbacks
    def test_process(self):
        # Also test the ReceiverProcess helper class
        p1 = ReceiverProcess()
        pid1 = yield p1.spawn()
        yield p1.init()

        processes = [
            {'name':'echo','module':'ion.core.test.test_baseprocess','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes, sup=p1)
        assert sup == p1

        pid2 = p1.get_child_id('echo')

        yield p1.send(pid2, 'echo','content123')
        logging.info('Sent echo message')

        msg = yield p1.await_message()
        logging.info('Received echo message')

        self.assertEquals(msg.payload['op'], 'result')
        self.assertEquals(msg.payload['content']['value'], 'content123')

        yield sup.shutdown()

    @defer.inlineCallbacks
    def test_send_byte_string(self):
        """
        @brief Test that any arbitrary byte string can be sent through the
        ion + magnet stack. Use a 20 byte sha1 digest as test string.
        """
        p1 = ReceiverProcess()
        pid1 = yield p1.spawn()
        yield p1.init()

        processes = [
            {'name':'echo','module':'ion.core.test.test_baseprocess','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes, sup=p1)

        pid2 = p1.get_child_id('echo')

        byte_string = sha.sha('test').digest()

        yield p1.send(pid2, 'echo', byte_string)
        logging.info('Sent byte-string')

        msg = yield p1.await_message()
        logging.info('Received byte-string')
        self.assertEquals(msg.payload['content']['value'], byte_string)

        yield sup.shutdown()



    @defer.inlineCallbacks
    def test_shutdown(self):
        processes = [
            {'name':'echo1','module':'ion.core.test.test_baseprocess','class':'EchoProcess'},
            {'name':'echo2','module':'ion.core.test.test_baseprocess','class':'EchoProcess'},
            {'name':'echo3','module':'ion.core.test.test_baseprocess','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes)

        yield self._shutdown_processes()


class EchoProcess(BaseProcess):
    @defer.inlineCallbacks
    def op_echo(self, content, headers, msg):
        logging.info("Message received: "+str(content))
        yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def op_echofail1(self, content, headers, msg):
        logging.info("Message received: "+str(content))
        ex = RuntimeError("I'm supposed to fail")
        yield self.reply_err(msg, ex)

    @defer.inlineCallbacks
    def op_echofail2(self, content, headers, msg):
        logging.info("Message received: "+str(content))
        raise RuntimeError("I'm supposed to fail")
        yield self.reply_ok(msg, content)

# Spawn of the process using the module name
factory = ProtocolFactory(EchoProcess)
