#!/usr/bin/env python

"""
@file ion/test/iontest.py
@author Michael Meisinger
@brief test case for ION integration and system test cases (and some unit tests)
"""

from twisted.trial import unittest
from twisted.internet import defer, reactor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import base_process, bootstrap, ioninit
from ion.core import ioninit
from ion.core.base_process import BaseProcess
from ion.core.cc import container
from ion.core.cc.container import Id
from ion.core.process.process import IProcess
from ion.data.store import Store
import ion.util.procutils as pu

from ion.resources import description_utility

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
    procRegistry = base_process.procRegistry

    @defer.inlineCallbacks
    def _start_container(self):
        """
        Starting and initialzing the container with a connection to a broker.
        """
        mopt = {}
        mopt['broker_host'] = CONF['broker_host']
        mopt['broker_port'] = CONF['broker_port']
        mopt['broker_vhost'] = CONF['broker_vhost']
        mopt['broker_heartbeat'] = CONF['broker_heartbeat']
        mopt['boot_script'] = None
        mopt['script'] = None

        self.container = container.create_new_container()
        yield self.container.initialize(mopt)
        yield self.container.activate()

        # Manually perform some ioncore initializations
        yield bootstrap.init_ioncore()

        self.procRegistry = base_process.procRegistry
        self.test_sup = yield bootstrap.create_supervisor()

        log.info("============ %s ===" % self.container)

    @defer.inlineCallbacks
    def _start_core_services(self):
        sup = yield bootstrap.spawn_processes(bootstrap.ion_core_services,
                                              self.test_sup)
        log.info("============Core ION services started============")
        defer.returnValue(sup)

    @defer.inlineCallbacks
    def _stop_container(self):
        """
        Taking down the container's connection to the broker an preparing for
        reinitialization.
        """
        log.info("Closing ION container")
        self.test_sup = None
        dcs = reactor.getDelayedCalls()
        log.info("Cancelling %s delayed reactor calls!" % len(dcs))
        for dc in dcs:
            # Cancel the registered delayed call (this is non-async)
            dc.cancel()
        yield self.container.terminate()
        bootstrap.reset_container()
        log.info("============ION container closed============")

    def _shutdown_processes(self, proc=None):
        """
        Shuts down spawned test processes.
        """
        if proc:
            return proc.shutdown()
        else:
            return self.test_sup.shutdown()

    def _declare_messaging(self, messaging):
        return bootstrap.declare_messaging(messaging)

    def _spawn_processes(self, procs, sup=None):
        sup = sup if sup else self.test_sup
        return bootstrap.spawn_processes(procs, sup)

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
        @retval BaseProcess instance for process id
        """
        process = ioninit.container_instance.proc_manager.process_registry.kvs.get(pid, None)
        return process

class ReceiverProcess(BaseProcess):
    """
    A simple process that can send messages and tracks all received
    messages
    """
    def __init__(self, *args, **kwargs):
        BaseProcess.__init__(self, *args, **kwargs)
        self.inbox = defer.DeferredQueue()
        self.inbox_count = 0

    def _dispatch_message(self, payload, msg, target, conv):
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
