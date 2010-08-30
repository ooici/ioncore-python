#!/usr/bin/env python

"""
@file ion/test/iontest.py
@author Michael Meisinger
@brief test case for ION integration and system test cases (and some unit tests)
"""

import logging
log = logging.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer, reactor
from magnet import container
from magnet.container import Id

from ion.core import base_process, bootstrap, ioninit
from ion.core import ioninit
from ion.core.base_process import BaseProcess
from ion.data.store import Store
import ion.util.procutils as pu

from ion.resources import description_utility


CONF = ioninit.config(__name__)

class IonTestCase(unittest.TestCase):
    """
    Extension of python unittest.TestCase and trial unittest.TestCase for the
    purposes of supporting ION tests with a container/AMQP based execution
    environment.
    Use this as a base case for your unit tests, e.g.
     class DatastoreTest(IonTestCase):
    """

    procRegistry = base_process.procRegistry

    @defer.inlineCallbacks
    def _start_container(self):
        """
        Starting and initialzing the container with a connection to a broker.
        @note Hardwired to connect to amoeba for broker.
        """
        mopt = {}
        mopt['broker_host'] = CONF['broker_host']
        mopt['broker_port'] = CONF['broker_port']
        mopt['broker_vhost'] = CONF['broker_vhost']
        mopt['broker_heartbeat'] = CONF['broker_heartbeat']
        mopt['boot_script'] = None
        mopt['script'] = None

        self.cont_conn = yield container.startContainer(mopt)
        bootstrap.init_container()
        self.procRegistry = base_process.procRegistry
        self.test_sup = yield bootstrap.create_supervisor()

        #Load All Resource Descriptions for future decoding
        description_utility.load_descriptions()

        log.info("============Magnet container started, "+repr(self.cont_conn))

    @defer.inlineCallbacks
    def _start_core_services(self):
        sup = yield bootstrap.spawn_processes(bootstrap.ion_core_services,
                                              self.test_sup)
        log.info("============Core ION services started============")
        defer.returnValue(sup)

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
            dc.cancel()
        self.cont_conn.transport.loseConnection()
        container.Container._started = False
        container.Container.store = Store()
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
        for rec in base_process.receivers:
            if rec.spawned.id.full == str(pid):
                return rec.procinst
        return None

class ReceiverProcess(BaseProcess):
    """
    A simple process that can send messages and tracks all received
    messages
    """
    def __init__(self, *args, **kwargs):
        BaseProcess.__init__(self, *args, **kwargs)
        self.inbox = defer.DeferredQueue()

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
        return defer.succeed(True)

    def await_message(self):
        """
        @retval Deferred for arriving message
        """
        return self.inbox.get()
