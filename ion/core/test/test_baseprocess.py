#!/usr/bin/env python

"""
@file ion/core/test/test_baseprocess.py
@author Michael Meisinger
@brief test case for process base class
"""

import os
import sha
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer

from magnet.container import Container
from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn

from ion.core import ioninit
from ion.core.base_process import BaseProcess, ProcessDesc, ProtocolFactory

from ion.test.iontest import IonTestCase, ReceiverProcess
import ion.util.procutils as pu

class BaseProcessTest(IonTestCase):
    """
    Tests the process base class, the root class of all message based interaction.
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
        self.assertEquals(p1.id, None)
        self.assertTrue(p1.receiver)
        self.assertEquals(p1.receiver.spawned, None)
        self.assertEquals(p1.proc_state, "NEW")

        pid1 = yield p1.spawn()
        self.assertEquals(pid1, p1.receiver.spawned.id)

        # Note: this tests init without actually sending a message.
        self.assertEquals(p1.proc_state, "ACTIVE")

        yield p1.op_init(None, None, None)
        self.assertEquals(p1.proc_state, "ERROR")

        rec = Receiver("myname")
        p2 = BaseProcess(rec)

        args = {'arg1':'value1','arg2':{}}
        p3 = BaseProcess(None, args)
        self.assertEquals(p3.spawn_args, args)

    @defer.inlineCallbacks
    def test_child_processes(self):
        p1 = BaseProcess()
        pid1 = yield p1.spawn()

        child = ProcessDesc(name='echo', module='ion.core.test.test_baseprocess')
        pid2 = yield p1.spawn_child(child)

        (cont,hdrs,msg) = yield p1.rpc_send(pid2,'echo','content123')
        self.assertEquals(cont['value'], 'content123')

        yield p1.shutdown()

    @defer.inlineCallbacks
    def test_spawn_child(self):
        child1 = ProcessDesc(name='echo', module='ion.core.test.test_baseprocess')
        self.assertEquals(child1.proc_state,'DEFINED')

        pid1 = yield self.test_sup.spawn_child(child1)
        self.assertEquals(child1.proc_state,'INIT_OK')
        proc = self._get_procinstance(pid1)
        self.assertEquals(str(proc.__class__),"<class 'ion.core.test.test_baseprocess.EchoProcess'>")
        self.assertEquals(pid1, proc.receiver.spawned.id)
        log.info('Process 1 spawned and initd correctly')

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(pid1,'echo','content123')
        self.assertEquals(cont['value'], 'content123')
        log.info('Process 1 responsive correctly')

        # The following tests the process attaching a second receiver
        msgName = self.test_sup.get_scoped_name('global', pu.create_guid())
        messaging = {'name_type':'worker', 'args':{'scope':'global'}}
        yield Container.configure_messaging(msgName, messaging)
        extraRec = Receiver(proc.proc_name, msgName)
        extraRec.handle(proc.receive)
        extraid = yield spawn(extraRec)
        log.info('Created new receiver %s with pid %s' % (msgName, extraid))

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(msgName,'echo','content456')
        self.assertEquals(cont['value'], 'content456')
        log.info('Process 1 responsive correctly on second receiver')


    @defer.inlineCallbacks
    def test_process(self):
        # Also test the ReceiverProcess helper class
        p1 = ReceiverProcess()
        pid1 = yield p1.spawn()

        processes = [
            {'name':'echo','module':'ion.core.test.test_baseprocess','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes, sup=p1)
        assert sup == p1

        pid2 = p1.get_child_id('echo')

        yield p1.send(pid2, 'echo','content123')
        log.info('Sent echo message')

        msg = yield p1.await_message()
        log.info('Received echo message')

        self.assertEquals(msg.payload['op'], 'result')
        self.assertEquals(msg.payload['content']['value'], 'content123')

        yield sup.shutdown()

    @defer.inlineCallbacks
    def test_message_before_init(self):
        child2 = ProcessDesc(name='echo', module='ion.core.test.test_baseprocess')
        pid2 = yield self.test_sup.spawn_child(child2, init=False)
        self.assertEquals(child2.proc_state, 'SPAWNED')
        proc2 = self._get_procinstance(pid2)

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(pid2,'echo','content123')
        self.assertEquals(cont['status'], 'ERROR')
        log.info('Process 1 rejected first message correctly')

        yield child2.init()
        self.assertEquals(child2.proc_state, 'INIT_OK')
        log.info('Process 1 rejected initialized OK')

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(pid2,'echo','content123')
        self.assertEquals(cont['value'], 'content123')
        log.info('Process 1 responsive correctly after init')

    @defer.inlineCallbacks
    def test_message_during_init(self):
        child2 = ProcessDesc(name='echo', module='ion.core.test.test_baseprocess')
        pid2 = yield self.test_sup.spawn_child(child2, init=False)
        proc2 = self._get_procinstance(pid2)
        proc2.plc_init = proc2.plc_noinit
        self.assertEquals(proc2.proc_state, 'NEW')

        msgName = self.test_sup.get_scoped_name('global', pu.create_guid())
        messaging = {'name_type':'worker', 'args':{'scope':'global'}}
        yield Container.configure_messaging(msgName, messaging)
        extraRec = Receiver(proc2.proc_name, msgName)
        extraRec.handle(proc2.receive)
        extraid = yield spawn(extraRec)
        log.info('Created new receiver %s with pid %s' % (msgName, extraid))

        yield self.test_sup.send(pid2, 'init',{},{'quiet':True})
        log.info('Sent init to process 1')

        yield pu.asleep(0.5)
        self.assertEquals(proc2.proc_state, 'INIT')

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(msgName,'echo','content123')
        self.assertEquals(cont['value'], 'content123')
        log.info('Process 1 responsive correctly after init')

        yield pu.asleep(2)
        self.assertEquals(proc2.proc_state, 'ACTIVE')

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(msgName,'echo','content123')
        self.assertEquals(cont['value'], 'content123')
        log.info('Process 1 responsive correctly after init')

    @defer.inlineCallbacks
    def test_error_in_op(self):
        child1 = ProcessDesc(name='echo', module='ion.core.test.test_baseprocess')
        pid1 = yield self.test_sup.spawn_child(child1)

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(pid1,'echofail2','content123')
        self.assertEquals(cont['status'], 'ERROR')
        log.info('Process 1 responded to error correctly')

    @defer.inlineCallbacks
    def test_send_byte_string(self):
        """
        @brief Test that any arbitrary byte string can be sent through the
        ion + magnet stack. Use a 20 byte sha1 digest as test string.
        """
        p1 = ReceiverProcess()
        pid1 = yield p1.spawn()

        processes = [
            {'name':'echo','module':'ion.core.test.test_baseprocess','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes, sup=p1)

        pid2 = p1.get_child_id('echo')

        byte_string = sha.sha('test').digest()

        yield p1.send(pid2, 'echo', byte_string)
        log.info('Sent byte-string')

        msg = yield p1.await_message()
        log.info('Received byte-string')
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
    def plc_noinit(self):
        log.info("In init: "+self.proc_state)
        yield pu.asleep(1)
        log.info("Leaving init: "+self.proc_state)

    @defer.inlineCallbacks
    def op_echo(self, content, headers, msg):
        log.info("Message received: "+str(content))
        yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def op_echofail1(self, content, headers, msg):
        log.info("Message received: "+str(content))
        ex = RuntimeError("I'm supposed to fail")
        yield self.reply_err(msg, ex)

    @defer.inlineCallbacks
    def op_echofail2(self, content, headers, msg):
        log.info("Message received: "+str(content))
        raise RuntimeError("I'm supposed to fail")
        yield self.reply_ok(msg, content)

# Spawn of the process using the module name
factory = ProtocolFactory(EchoProcess)
