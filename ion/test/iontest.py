#!/usr/bin/env python

"""
@file ion/test/iontest.py
@author Michael Meisinger
@brief test case for ION integration and system test cases (and some unit tests)
"""

import logging

from twisted.trial import unittest
from twisted.internet import defer, reactor
from magnet import container
from magnet.container import Id

from ion.core import base_process, bootstrap, ioninit
from ion.core import ioninit
from ion.data.store import Store
import ion.util.procutils as pu

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
        mopt['boot_script'] = None
        mopt['script'] = None

        self.cont_conn = yield container.startContainer(mopt)
        bootstrap.init_container()
        self.procRegistry = base_process.procRegistry
        logging.info("============Magnet container started, "+repr(self.cont_conn))

    @defer.inlineCallbacks
    def _start_core_services(self):
        sup = yield bootstrap.bootstrap(None, bootstrap.ion_core_services)
        logging.info("============Core ION services started============")
        defer.returnValue(sup)

    def _stop_container(self):
        """
        Taking down the container's connection to the broker an preparing for
        reinitialization.
        """
        logging.info("Closing ION container")
        dcs = reactor.getDelayedCalls()
        logging.info("Cancelling %s delayed reactor calls!" % len(dcs))
        for dc in dcs:
            dc.cancel()
        self.cont_conn.transport.loseConnection()
        container.Container._started = False
        container.Container.store = Store()
        bootstrap.reset_container()
        logging.info("============ION container closed============")


    def _declare_messaging(self, messaging):
        return bootstrap.declare_messaging(messaging)

    def _spawn_processes(self, procs):
        return bootstrap.spawn_processes(procs)

    def _get_procid(self, name):
        """
        @param name  process instance label given when spawning
        @retval process id of the process (locally) identified by name
        """
        return self.procRegistry.get(name)
