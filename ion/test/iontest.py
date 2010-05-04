#!/usr/bin/env python

"""
@file ion/test/iontest.py
@author Michael Meisinger
@brief test case for ION integration and system test cases (and some unit tests)
"""

import logging

from twisted.trial import unittest
from twisted.internet import defer

from magnet import container
from magnet.container import Id
from magnet.store import Store

from ion.core import base_process, bootstrap
from ion.core import ioninit
import ion.util.procutils as pu

class IonTestCase(unittest.TestCase):
    """
    Extension of python unittest.TestCase and trial unittest.TestCase for the
    purposes of supporting ION tests with a container/AMQP based execution
    environment
    """

    procRegistry = base_process.procRegistry

    @defer.inlineCallbacks
    def _startContainer(self):
        mopt = {}
        mopt['broker_host'] = 'amoeba.ucsd.edu'
        mopt['broker_port'] = 5672
        mopt['broker_vhost'] = '/'
        mopt['boot_script'] = None
        mopt['script'] = None
 
        self.cont_conn = yield container.startContainer(mopt)
        bootstrap.init_container()
        self.procRegistry = base_process.procRegistry
        logging.info("============Magnet container started, "+repr(self.cont_conn))
    
    _startMagnet = _startContainer

    @defer.inlineCallbacks
    def _startCoreServices(self):
        yield bootstrap.bootstrap(None, bootstrap.ion_core_services)
        logging.info("============Core ION services started============")

    def _stopContainer(self):
        logging.info("Closing ION container")
        self.cont_conn.transport.loseConnection()
        container.Container._started = False
        container.Container.store = Store()
        bootstrap.reset_container()
        logging.info("============ION container closed============")

    _stopMagnet = _stopContainer

    @defer.inlineCallbacks
    def _declareMessaging(self, messaging):
        yield bootstrap.bs_messaging(messaging)
    
    @defer.inlineCallbacks
    def _spawnProcesses(self, procs):
        yield bootstrap.bs_processes(procs)
