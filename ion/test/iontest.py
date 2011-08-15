#!/usr/bin/env python

"""
@file ion/test/iontest.py
@author Michael Meisinger
@author Matt Rodriguez
@brief test case for ION integration and system test cases (and some unit tests)
"""
import os

from twisted.trial import unittest
from twisted.internet import defer, reactor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import bootstrap
from ion.core import ioninit
from ion.core.cc import service
from ion.core.cc.container import Id, Container
from ion.core.messaging.receiver import Receiver
from ion.core.process import process
from ion.core.process.process import Process
from ion.core.data.store import Store
import ion.util.procutils as pu


# The following modules must be imported here, because they load config
# files. If done while in test, it does not work!

CONF = ioninit.config(__name__)

class IonTestCase(unittest.TestCase):
    """
    Extension of python unittest.TestCase and trial unittest.TestCase for the
    purposes of supporting ION tests with a container/AMQP based execution
    environment.
    Use this as a base case for your unit tests, e.g.
     class DatastoreTest(IonTestCase):
    """

    # Set timeout for Trial tests
    timeout = 20
    procRegistry = process.procRegistry
    container = None
    twisted_container_service = None #hack

    @defer.inlineCallbacks
    def _start_container(self, sysname=None, start_apps=None):
        """
        Starting and initialzing the container with a connection to a broker.
        """
        mopt = service.Options()
        if os.environ.has_key("ION_TEST_CASE_BROKER_HOST"):
            host = os.environ["ION_TEST_CASE_BROKER_HOST"]
            log.debug("Using environment set ION_TEST_CASE_BROKER_HOST (%s)" % host)
            mopt["broker_host"] = host
        else:
            mopt['broker_host'] = CONF['broker_host']
        mopt['broker_port'] = CONF['broker_port']
        mopt['broker_vhost'] = CONF['broker_vhost']
        mopt['broker_heartbeat'] = CONF['broker_heartbeat']
        mopt['broker_credfile'] = CONF.getValue('broker_credfile', None)
        mopt['lockfile'] = CONF.getValue('lockfile', None)
        mopt['no_shell'] = True
        mopt['no_dbmanhole'] = True
        # This is where dependent apps can be included
        if start_apps and type(start_apps) in (tuple, list):
            apps = []
            for app in start_apps:
                apps.append("res/apps/%s.app" % app)
            mopt['scripts'] = apps
        elif CONF['start_app']:
            mopt['scripts'] = [CONF['start_app']]

        # Little trick to have no consecutive failures if previous setUp() failed
        # @note This is not fail fast and does not always work. TEMPORARY.
        if Container._started:
            log.error("PROBLEM: Previous test did not stop container. Fixing...")
            yield self._stop_container()

        # Start container by simulating twistd service instantiation
        twisted_container_service = service.CapabilityContainer(mopt)
        yield twisted_container_service.startService()
        self.twisted_container_service = twisted_container_service

        # Manually perform some ioncore initializations
        if sysname == None and os.environ.has_key("ION_TEST_CASE_SYSNAME"):
            sysname = os.environ["ION_TEST_CASE_SYSNAME"]

        # save off the old container args
        self._old_container_args = None
        self._reset_container_args = False
        if sysname != None:
            log.info("Setting _start_container sysname to %s" % sysname)
            self._old_container_args = Container.args
            self._reset_container_args = True
            curargs = ''
            if Container.args:
                curargs = Container.args + ' '

            Container.args = curargs + "sysname=%s" % sysname       # making this last will override sysname if it's set twice due to parsing

        yield bootstrap.init_ioncore()      # calls set_container_args

        self.procRegistry = process.procRegistry
        self.test_sup = yield bootstrap.create_supervisor()

        log.info("============ %s UP ===" % ioninit.container_instance)

    @defer.inlineCallbacks
    def create_message(self, MessageContentTypeID, MessageName='', **kwargs):
        """
        @Brief This is a convienence method for creating message instances in a
        test case. The message instance is an envelope for application specific
        messages. The IonTest uses the message client built into the test
        supervisor process.
        @param MessageContentTypeID is the type of message to go in the message instance
        @param MessageName is a depricated concept in the architecture. Do not use it.
        @param kwargs The IonTest method allows key word arguments which will
        attempt to set fields in the message object. The message client does not
        provide this capability, but it could be added if it is helpful.
        """
        msg_instance = yield self.test_sup.message_client.create_instance(MessageContentTypeID, MessageName)
        for k,v in kwargs.items():
            setattr(msg_instance,k,v)
        defer.returnValue(msg_instance)

    @defer.inlineCallbacks
    def _stop_container(self):
        """
        Taking down the container's connection to the broker an preparing for
        reinitialization.
        """
        log.debug("============Closing ION container============")
        if self.twisted_container_service: #hack
            if self.twisted_container_service.running:
                yield self.twisted_container_service.stopService()

        self.test_sup = None
        # Cancel any delayed calls, such as timeouts, looping calls etc.
        dcs = reactor.getDelayedCalls()
        if len(dcs) > 0:
            log.debug("Cancelling %s delayed reactor calls!" % len(dcs))
        for dc in dcs:
            # Cancel the registered delayed call (this is non-async)
            dc.cancel()

        # The following is waiting a bit for any currently consumed messages
        # It also prevents the arrival of new messages
        Receiver.rec_shutoff = True
        msgstr = ""
        if len(Receiver.rec_messages) > 0:
            for msg in Receiver.rec_messages.values():
                msgstr += str(msg.payload) + ", \n"
            #log.warn("Content rec_messages: "+str(Receiver.rec_messages))
            log.warn("%s messages still being processed: %s" % (len(Receiver.rec_messages), msgstr))

        num_wait = 0
        while len(Receiver.rec_messages) > 0 and num_wait<10:
            yield pu.asleep(0.2)
            num_wait += 1

        #if self.container:
        #    yield self.container.terminate()
        #elif ioninit.container_instance:
        #    yield ioninit.container_instance.terminate()

        # fix Container args if we messed with them
        if hasattr(self, '_reset_container_args'):
            if self._reset_container_args:
                Container.args = self._old_container_args
                self._old_container_args = None
                self._reset_container_args = False
                # bootstrap.reset_container() will kill the stuff that bootstrap._set_container_args (called by bootstrap.init_ioncore) will set

        # Reset static module values back to initial state for next test case
        bootstrap.reset_container()

        log.info("============ION container closed============")

        if msgstr:
            raise RuntimeError("Unexpected message processed during container shutdown: "+msgstr)

    def _shutdown_processes(self, proc=None):
        """
        Shuts down spawned test processes.
        """
        log.debug("------------Shutting down test processes------------")
        if proc and proc != self.test_sup:
            return proc.shutdown()
        else:
            return self.test_sup.shutdown_child_procs()

    def _spawn_processes(self, procs, sup=None):
        sup = sup if sup else self.test_sup
        d = bootstrap.spawn_processes(procs, sup)
        def spawn_error(reason):
            """need to stopService here because by the next test case uses
            a new instance of this class, which means we don't have
            twisted_container_service anymore
            """
            d = self.twisted_container_service.stopService()
            d.addBoth(lambda _: defer.fail(reason))
            return d
        d.addErrback(spawn_error)
        return d

    def _spawn_process(self, process):
        return process.spawn()

    def _get_procid(self, name):
        """
        @param name  process instance label given when spawning
        @retval process id of the process (locally) identified by name
        """
        return self.procRegistry.get(name)

    def _get_procinstance(self, pid):
        """
        @param pid  process id
        @retval Process instance for process id
        """
        process = ioninit.container_instance.proc_manager.process_registry.kvs.get(pid, None)
        return process

    def _get_service_by_name(self, name):
        ''' Testcase utility to get directly to a ServiceProcess for monkey-patching, etc. '''
        pid = self.procRegistry.kvs.get(name)
        proc = self.test_sup.get_child_def(name)
        return proc.container.proc_manager.process_registry.kvs.get(pid, None)

    def run(self, result):
        # TODO This is where we can inject user id and expiry into test case
        # stack to allow running of tests within the policy enforcement environment
        # as well as force certain policy enforcement failures, etc.

        unittest.TestCase.run(self, result)


class ReceiverProcess(Process):
    """
    A simple process that can send messages and tracks all received
    messages
    """
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self.inbox = defer.DeferredQueue()
        self.inbox_count = 0

    def _dispatch_message(self, payload, msg, conv):
        """
        Dispatch of messages to operations within this process instance. The
        default behavior is to dispatch to 'op_*' functions, where * is the
        'op' message attribute.
        @retval deferred
        """
        log.info('ReceiverProcess: Received message op=%s from sender=%s' %
                     (msg.payload['op'], msg.payload['sender']))
        self.inbox.put(msg)
        self.inbox_count += 1
        return defer.succeed(True)

    def await_message(self):
        """
        @retval Deferred for arriving message
        """
        return self.inbox.get()

# Stuff for testing: Stubs, mock objects
fakeStore = Store()

class FakeMessage(object):
    """Instances of this object are given to receive functions and handlers
    by test cases, in lieu of carrot BaseMessage instances. Production code
    detects these and no send is done.
    """
    def __init__(self, payload=None):
        self.payload = payload

    @defer.inlineCallbacks
    def send(self, to, msg):
        self.sendto = to
        self.sendmsg = msg
        # Need to be a generator
        yield fakeStore.put('fake','fake')

class FakeSpawnable(object):
    def __init__(self, id=None):
        self.id = id or Id('fakec','fakep')

class FakeReceiver(object):
    """Instances of this object are given to send/spawn functions
    by test cases, in lieu of ion.core.messaging.receiver.Receiver instances.
    Production code detects these and no send is done.
    """
    def __init__(self, id=None):
        self.payload = None
        self.process = FakeSpawnable()
        self.group = 'fake'

    @defer.inlineCallbacks
    def send(self, to, msg):
        self.sendto = to
        self.sendmsg = msg
        # Need to be a generator
        yield fakeStore.put('fake','fake')


