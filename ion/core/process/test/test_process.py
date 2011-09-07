#!/usr/bin/env python

"""
@file ion/core/process/test/test_process.py
@author Michael Meisinger
@brief test case for process base class
"""

import ion.core.ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import os
import hashlib

from twisted.trial import unittest
from twisted.internet import defer


from ion.core.process import service_process
from ion.core.process.process import Process, ProcessDesc, ProcessFactory, ProcessError
from ion.core.cc.container import Container
from ion.core.exception import ReceivedContainerError, ReceivedApplicationError, ApplicationError, ReceivedError
from ion.core.messaging.receiver import Receiver, WorkerReceiver
from ion.core.id import Id
from ion.test.iontest import IonTestCase, ReceiverProcess
import ion.util.procutils as pu

from ion.core.process.test import life_cycle_process
from ion.util import state_object

class ProcessTest(IonTestCase):
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
        p1 = Process()
        self.assertTrue(p1.id)
        self.assertIsInstance(p1.id, Id)
        self.assertTrue(p1.receiver)
        self.assertFalse(p1.receiver.consumer)
        self.assertEquals(p1.receiver.consumer, None)
        self.assertEquals(p1._get_state(), "INIT")

        self.assertEquals(p1.spawn_args, {})
        self.assertTrue(p1.proc_init_time)
        self.assertTrue(p1.proc_name)
        self.assertTrue(p1.sys_name)
        self.assertTrue(p1.proc_group)
        self.assertTrue(p1.backend_id)
        self.assertTrue(p1.backend_receiver)
        self.assertEquals(len(p1.receivers), 2)
#        self.assertEquals(p1.conversations, {})
        self.assertEquals(p1.child_procs, [])

        pid1 = yield p1.spawn()
        self.assertEquals(pid1, p1.id)
        self.assertEquals(p1._get_state(), "ACTIVE")

        procid = Id('local','container')
        args = {'proc-id':procid.full}
        p2 = Process(spawnargs=args)
        self.assertEquals(p2.id, procid)
        yield p2.initialize()
        self.assertEquals(p2._get_state(), "READY")
        yield p2.activate()
        self.assertEquals(p2._get_state(), "ACTIVE")

        args = {'arg1':'value1','arg2':{}}
        p3 = Process(None, args)
        self.assertEquals(p3.spawn_args, args)

    @defer.inlineCallbacks
    def test_process(self):
        # Also test the ReceiverProcess helper class
        log.debug('Spawning p1')
        p1 = ReceiverProcess(spawnargs={'proc-name':'p1'})
        pid1 = yield p1.spawn()

        log.debug('Spawning other processes')
        processes = [
            {'name':'echo','module':'ion.core.process.test.test_process','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes, sup=p1)
        assert sup == p1

        pid2 = p1.get_child_id('echo')
        proc2 = self._get_procinstance(pid2)

        ## This here simulates an RPC without calling the rpc_send(). Problem!
        #hrds = {'performative':'request', 'protocol':'rpc', 'conv-id':'#1'}
        #yield p1.send(pid2, 'echo', 'content123', hrds)
        #log.info('Sent echo message')
        #
        #msg = yield p1.await_message()
        #log.info('Received echo message')
        #
        #self.assertEquals(msg.payload['op'], 'result')
        ##self.assertEquals(msg.payload['content']['value'], 'content123')
        #self.assertEquals(msg.payload['content'], 'content123')

        yield sup.terminate()
        self.assertEquals(sup._get_state(), "TERMINATED")
        self.assertEquals(proc2._get_state(), "TERMINATED")


    @defer.inlineCallbacks
    def test_child_processes(self):
        p1 = Process()
        pid1 = yield p1.spawn()

        child = ProcessDesc(name='echo', module='ion.core.process.test.test_process')
        pid2 = yield p1.spawn_child(child)

        (cont,hdrs,msg) = yield p1.rpc_send(pid2,'echo','content123')
        log.debug("Received RPC response from echo process: %s" % cont)
        #self.assertEquals(cont['value'], 'content123')
        self.assertEqual(hdrs.get(p1.MSG_STATUS),'OK')
        self.assertEquals(cont, 'content123')

        log.info("--- Test successful, terminating echo process now")

        yield p1.terminate()
        self.assertEquals(p1._get_state(), "TERMINATED")
        log.info("Echo terminated")

    @defer.inlineCallbacks
    def test_spawn_child(self):
        child1 = ProcessDesc(name='echo', module='ion.core.process.test.test_process')
        self.assertEquals(child1._get_state(),'INIT')

        pid1 = yield self.test_sup.spawn_child(child1)
        self.assertEquals(child1._get_state(),'ACTIVE')
        proc = self._get_procinstance(pid1)
        self.assertEquals(str(proc.__class__),"<class 'ion.core.process.test.test_process.EchoProcess'>")
        self.assertEquals(pid1, proc.id)
        log.info('Process 1 spawned and initd correctly')


        (cont,hdrs,msg) = yield self.test_sup.rpc_send(pid1,'echo','content123')

        #self.assertEquals(cont['value'], 'content123')
        self.assertEquals(cont, 'content123')
        log.info('Process 1 responsive correctly')

        # The following tests the process attaching a second receiver
        msgName = pu.create_guid()
        extraRec = WorkerReceiver(label=proc.proc_name, name=msgName, handler=proc.receive)
        extraid = yield extraRec.attach()
        log.info('Created new receiver %s' % (msgName))

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(msgName,'echo','content456')
        #self.assertEquals(cont['value'], 'content456')
        self.assertEquals(cont, 'content456')
        log.info('Process 1 responsive correctly on second receiver')


    @defer.inlineCallbacks
    def xtest_message_before_activate(self):
        p1 = ReceiverProcess(spawnargs={'proc-name':'p1'})
        pid1 = yield p1.spawn()
        proc1 = self._get_procinstance(pid1)

        child2 = ProcessDesc(name='echo', module='ion.core.process.test.test_process')
        pid2 = yield self.test_sup.spawn_child(child2, activate=False)
        self.assertEquals(child2._get_state(), 'READY')
        proc2 = self._get_procinstance(pid2)
        self.assertEquals(proc2._get_state(), 'READY')

        # The following tests that a message to a not yet activated process
        # is queued and not lost, but not delivered
        yield proc1.send(pid2,'echo','content123')
        self.assertEquals(proc1.inbox_count, 0)
        yield pu.asleep(1)
        self.assertEquals(proc1.inbox_count, 0)

        yield child2.activate()
        yield pu.asleep(1)
        self.assertEquals(child2._get_state(), 'ACTIVE')
        self.assertEquals(proc1.inbox_count, 1)

        (cont,hdrs,msg) = yield self.test_sup.rpc_send(pid2,'echo','content123')
        #self.assertEquals(cont['value'], 'content123')
        self.assertEquals(cont, 'content123')
        log.info('Process 1 responsive correctly after init')


    @defer.inlineCallbacks
    def test_echo(self):
        child1 = ProcessDesc(name='echo', module='ion.core.process.test.test_process')
        pid1 = yield self.test_sup.spawn_child(child1)

        send_content = 'content123'
        (result_content,hdrs,msg) = yield self.test_sup.rpc_send(pid1,'echo',send_content)
        self.assertEqual(result_content, send_content)

    @defer.inlineCallbacks
    def test_echo_fail(self):
        child1 = ProcessDesc(name='echo', module='ion.core.process.test.test_process')
        pid1 = yield self.test_sup.spawn_child(child1)

        yield self.failUnlessFailure(self.test_sup.rpc_send(pid1,'echo_fail','content123'), ReceivedApplicationError)


    @defer.inlineCallbacks
    def test_echo_exception(self):
        child1 = ProcessDesc(name='echo', module='ion.core.process.test.test_process')
        pid1 = yield self.test_sup.spawn_child(child1)

        yield self.failUnlessFailure(self.test_sup.rpc_send(pid1,'echo_exception','content123'), ReceivedContainerError)



    @defer.inlineCallbacks
    def test_echo_apperror(self):
        child1 = ProcessDesc(name='echo', module='ion.core.process.test.test_process')
        pid1 = yield self.test_sup.spawn_child(child1)

        yield self.failUnlessFailure(self.test_sup.rpc_send(pid1,'echo_apperror','content123'), ReceivedApplicationError)


    @defer.inlineCallbacks
    def test_send_byte_string(self):
        """
        @brief Test that any arbitrary byte string can be sent through the
        ion CC stack. Use a 20 byte sha1 digest as test string.
        """
        p1 = ReceiverProcess()
        pid1 = yield p1.spawn()

        processes = [
            {'name':'echo','module':'ion.core.process.test.test_process','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes, sup=p1)

        pid2 = p1.get_child_id('echo')

        byte_string = hashlib.sha1('test').digest()

        (cont,hdrs,msg) = yield p1.rpc_send(pid2, 'echo', byte_string)

        self.assertEquals(msg.payload['content'], byte_string)

        yield sup.shutdown()

    @defer.inlineCallbacks
    def test_shutdown(self):
        processes = [
            {'name':'echo1','module':'ion.core.process.test.test_process','class':'EchoProcess'},
            {'name':'echo2','module':'ion.core.process.test.test_process','class':'EchoProcess'},
            {'name':'echo3','module':'ion.core.process.test.test_process','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes)

        yield self._shutdown_processes()

    @defer.inlineCallbacks
    def test_rpc_timeout(self):
        sup = self.test_sup
        try:
            yield sup.rpc_send('big_void', 'noop', 'arbitrary', timeout=1)
            self.fail("TimeoutError expected")
        except defer.TimeoutError, te:
            log.info('Timeout received')

    @defer.inlineCallbacks
    def test_register_lco(self):
        """
        Test the registration of life cycle objects owned by a process
        Do not spawn the process - want to manually move it through the FSM!
        """
        # Create a process which has an lco object in its init
        lco1 = life_cycle_process.LifeCycleObject()
        lcop = life_cycle_process.LCOProcess(lco1, spawnargs={'proc-name':'p1'})
        self.assertEquals(lcop._get_state(), state_object.BasicStates.S_INIT)
        self.assertEquals(lco1._get_state(), state_object.BasicStates.S_INIT)

        lco2 = life_cycle_process.LifeCycleObject()
        yield lcop.register_life_cycle_object(lco2)
        self.assertEquals(lco2._get_state(), state_object.BasicStates.S_INIT)

        # Initialize the process and its objects
        yield lcop.initialize()
        self.assertEquals(lcop._get_state(), state_object.BasicStates.S_READY)
        self.assertEquals(lco1._get_state(), state_object.BasicStates.S_READY)
        self.assertEquals(lco2._get_state(), state_object.BasicStates.S_READY)

        lco3 = life_cycle_process.LifeCycleObject()
        yield lcop.register_life_cycle_object(lco3)
        self.assertEquals(lco3._get_state(), state_object.BasicStates.S_READY)

        # Check that using add after init causes an error
        lcoa = life_cycle_process.LifeCycleObject()
        self.assertRaises(ProcessError,lcop.add_life_cycle_object,lcoa)

        # Activate the process and its objects
        yield lcop.activate()

        self.assertEquals(lcop._get_state(), state_object.BasicStates.S_ACTIVE)
        self.assertEquals(lco1._get_state(), state_object.BasicStates.S_ACTIVE)
        self.assertEquals(lco2._get_state(), state_object.BasicStates.S_ACTIVE)
        self.assertEquals(lco3._get_state(), state_object.BasicStates.S_ACTIVE)


        lco4 = life_cycle_process.LifeCycleObject()
        yield lcop.register_life_cycle_object(lco4)
        self.assertEquals(lco4._get_state(), state_object.BasicStates.S_ACTIVE)

        # Process does not currently implement for deactivate!

        # Terminate the process and its objects
        yield lcop.terminate()
        self.assertEquals(lcop._get_state(), state_object.BasicStates.S_TERMINATED)
        self.assertEquals(lco1._get_state(), state_object.BasicStates.S_TERMINATED)
        self.assertEquals(lco2._get_state(), state_object.BasicStates.S_TERMINATED)
        self.assertEquals(lco3._get_state(), state_object.BasicStates.S_TERMINATED)
        self.assertEquals(lco4._get_state(), state_object.BasicStates.S_TERMINATED)

        # Can't seem to assert raises - not sure why not?
        #lco5 = life_cycle_process.LifeCycleObject()
        #self.assertRaises(ProcessError,lcop.register_life_cycle_object,lco5)
        #yield lcop.register_life_cycle_object(lco5)

    @defer.inlineCallbacks
    def test_register_lco_during_transition(self):
        # test a process which registers lcos during init/activate
        lcap = life_cycle_process.LCOProcessAddingObjects()
        self.failUnlessEquals(len(lcap._registered_life_cycle_objects), 1)      # the 1 is the LCC event publisher
        self.failUnlessEquals(lcap._get_state(), state_object.BasicStates.S_INIT)

        yield lcap.initialize()
        self.failUnlessEquals(lcap._get_state(), state_object.BasicStates.S_READY)
        self.failUnlessEquals(lcap._obj_init._get_state(), state_object.BasicStates.S_READY)

        yield lcap.activate()
        self.failUnlessEquals(lcap._get_state(), state_object.BasicStates.S_ACTIVE)
        self.failUnlessEquals(lcap._obj_init._get_state(), state_object.BasicStates.S_ACTIVE)
        self.failUnlessEquals(lcap._obj_activate._get_state(), state_object.BasicStates.S_ACTIVE)

    @defer.inlineCallbacks
    def test_register_lco_terminate_on_error(self):
        # test registered lcos with an owning process that Errors - should nicely advance each registered LCO
        # to the TERMINATED state upon error
        ep = DeadOnActivateProcess()

        eplco1 = life_cycle_process.LifeCycleObject()
        eplco2 = life_cycle_process.LifeCycleObject()
        eplco3 = life_cycle_process.LifeCycleObject()

        ep.register_life_cycle_object(eplco1)
        ep.register_life_cycle_object(eplco2)
        ep.register_life_cycle_object(eplco3)

        # calls initialize, activate, DIES
        try:
            yield ep.spawn()
        except:
            pass

        self.failUnlessEquals(ep._get_state(), state_object.BasicStates.S_ERROR)
        self.failUnlessEquals(eplco1._get_state(), state_object.BasicStates.S_TERMINATED)
        self.failUnlessEquals(eplco2._get_state(), state_object.BasicStates.S_TERMINATED)
        self.failUnlessEquals(eplco3._get_state(), state_object.BasicStates.S_TERMINATED)

    @defer.inlineCallbacks
    def test_register_lco_terminate_on_error_with_misbehaving_lco(self):
        # test registered LCOs with an owning process that Errors: this time we have a misbehaving LCO
        # that won't terminate properly
        ep2 = DeadOnActivateProcess()

        ep2lco1 = life_cycle_process.LifeCycleObject()
        ep2lco2 = life_cycle_process.LifeCycleObject()
        ep2lco3 = MisbehavingLCO()

        ep2.register_life_cycle_object(ep2lco1)
        ep2.register_life_cycle_object(ep2lco2)
        ep2.register_life_cycle_object(ep2lco3)

        # calls initialize, activate, DIES
        try:
            yield ep2.spawn()
        except:
            pass

        self.failUnlessEquals(ep2._get_state(), state_object.BasicStates.S_ERROR)
        self.failUnlessEquals(ep2lco1._get_state(), state_object.BasicStates.S_TERMINATED)
        self.failUnlessEquals(ep2lco2._get_state(), state_object.BasicStates.S_TERMINATED)
        self.failUnlessEquals(ep2lco3._get_state(), state_object.BasicStates.S_ERROR)      # did not terminate

        # test registered LCOs with an owning process that Errors: this time we have a misbehaving LCO in the middle
        # of the registered LCOs
        ep3 = DeadOnActivateProcess()

        ep3lco1 = life_cycle_process.LifeCycleObject()
        ep3lco2 = MisbehavingLCO()
        ep3lco3 = life_cycle_process.LifeCycleObject()

        ep3.register_life_cycle_object(ep3lco1)
        ep3.register_life_cycle_object(ep3lco2)
        ep3.register_life_cycle_object(ep3lco3)

        # calls initialize, activate, DIES
        try:
            yield ep3.spawn()
        except:
            pass

        self.failUnlessEquals(ep3._get_state(), state_object.BasicStates.S_ERROR)
        self.failUnlessEquals(ep3lco1._get_state(), state_object.BasicStates.S_TERMINATED)
        self.failUnlessEquals(ep3lco2._get_state(), state_object.BasicStates.S_ERROR)       # did not terminate
        self.failUnlessEquals(ep3lco3._get_state(), state_object.BasicStates.S_TERMINATED)  # terminated fine as its in parallel

class DeadOnActivateProcess(Process):
    """
    Simple testing class to put it in the error state.
    """
    def on_activate(self, *args, **kwargs):
        raise StandardError("oh noes, I did what I said I would do!")

class MisbehavingLCO(life_cycle_process.LifeCycleObject):
    def on_terminate(self, *args, **kwargs):
        raise StandardError("I don't want to terminate!")

class EchoProcess(Process):

    @defer.inlineCallbacks
    def op_echo(self, content, headers, msg):
        log.info("Message received: "+str(content))
        yield self.reply_ok(msg, content=content)
        log.info("Reply Sent")


    @defer.inlineCallbacks
    def op_echo_fail(self, content, headers, msg):
        log.info("Message received: "+str(content))
        ex = RuntimeError("I'm supposed to fail")
        # Reply as though we caught an exception!
        yield self.reply_err(msg,content=None, exception=ex, response_code=self.BAD_REQUEST)

    @defer.inlineCallbacks
    def op_echo_exception(self, content, headers, msg):
        log.info("Message received: "+str(content))
        raise RuntimeError("I'm supposed to fail: Cause a container Error")


    @defer.inlineCallbacks
    def op_echo_apperror(self, content, headers, msg):
        log.info("Message received: "+str(content))
        raise ApplicationError("I'm supposed to fail: Cause an application error", self.BAD_REQUEST)


# Spawn of the process using the module name
factory = ProcessFactory(EchoProcess)

class DummyClient(service_process.ServiceClient):

    def __init__(self, targetname='dummy', proc=None, **kw):
        kw['targetname'] = targetname
        service_process.ServiceClient.__init__(self, proc, **kw)

class ServiceClientTest(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        s = [
                {
                    'name':'logger', 
                    'module':'ion.services.coi.logger',
                    'class':'LoggerService'
                    }
                ]
        yield self._start_container()
        yield self._spawn_processes(s)

    @defer.inlineCallbacks
    def test_does_service_exist_true(self):
        client = DummyClient()
        a = yield client.does_service_exist('logger')
        self.failUnless(a)

    @defer.inlineCallbacks
    def test_does_service_exist_false(self):
        client = DummyClient()
        a = yield client.does_service_exist('nonexistent')
        self.failIf(a)
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


class ProcessContextTest(IonTestCase):
    """
    Test the threaded context model

    Consider expanding test to use a version of echo that allows inspection while handling the message... ie yield on
    a defered that is fired by the test case...
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_rpc_ok(self):
        processes = [
            {'name':'echo1','module':'ion.core.process.test.test_process','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes)
        pid = sup.get_child_id('echo1')

        proc = Process()
        yield proc.spawn()

        mc = proc.message_client

        message = yield mc.create_instance(None) # empty message - keep it simple

        log.info('Request Message Repo before send: %s' % message.Repository)
        self.assertEqual(message.Repository.convid_context, 'Default Context')

        (response, headers, msg) = yield proc.rpc_send(pid, 'echo', message)

        log.info('Request Message Repo after send: %s' % message.Repository)
        log.info('Response Message Repo after send: %s' % response.Repository)

        self.assertEqual(message.Repository.convid_context, 'Default Context')
        self.assertEqual(response.Repository.convid_context, 'Default Context')


    @defer.inlineCallbacks
    def test_rpc_apperror(self):
        processes = [
            {'name':'echo1','module':'ion.core.process.test.test_process','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes)
        pid = sup.get_child_id('echo1')


        proc = Process()
        yield proc.spawn()

        mc = proc.message_client

        message = yield mc.create_instance(None) # empty message - keep it simple

        log.info('Request Message Repo before send: %s' % message.Repository)
        self.assertEqual(message.Repository.convid_context, 'Default Context')

        try:
            (response, headers, msg) = yield proc.rpc_send(pid, 'echo_apperror', message)

        except ReceivedError, re:

            log.info('Request Message Repo after send: %s' % message.Repository)
            log.info('Response Message Repo after send: %s' % re.msg_content.Repository)

            self.assertEqual(message.Repository.convid_context, 'Default Context')
            self.assertEqual(re.msg_content.Repository.convid_context, 'Default Context')


    @defer.inlineCallbacks
    def test_rpc_container_error(self):
        processes = [
            {'name':'echo1','module':'ion.core.process.test.test_process','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes)
        pid = sup.get_child_id('echo1')


        proc = Process()
        yield proc.spawn()

        mc = proc.message_client

        message = yield mc.create_instance(None) # empty message - keep it simple

        log.info('Request Message Repo before send: %s' % message.Repository)
        self.assertEqual(message.Repository.convid_context, 'Default Context')

        try:
            (response, headers, msg) = yield proc.rpc_send(pid, 'echo_exception', message)

        except ReceivedError, re:

            log.info('Request Message Repo after send: %s' % message.Repository)
            log.info('Response Message Repo after send: %s' % re.msg_content.Repository)

            self.assertEqual(message.Repository.convid_context, 'Default Context')
            self.assertEqual(re.msg_content.Repository.convid_context, 'Default Context')
