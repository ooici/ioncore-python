#!/usr/bin/env python

"""
@file ion/test/iontest.py
@author Michael Meisinger
@brief test case for ION integration and system test cases (and some unit tests)
"""

import logging

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, DeferredQueue

from magnet import container
from magnet.service import Magnet
from magnet.service import Options

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.core import bootstrap
from ion.core import ioninit
import ion.util.procutils as pu

class IonTestCase(unittest.TestCase):
    """
    Extension of python unittest.TestCase and trial unittest.TestCase for the
    purposes of supporting ION tests with a container/AMQP based execution
    environment
    """
    
    def setUp(self):
        pass

    @defer.inlineCallbacks
    def _startMagnet(self):
        mopt = {}
        mopt['broker_host'] = '10.211.55.3'
        #mopt['broker_host'] = 'amoeba.ucsd.edu'
        mopt['broker_port'] = 5672
        mopt['broker_vhost'] = '/'
        mopt['boot_script'] = None
        mopt['script'] = None
 
        self.cont_conn = yield container.startContainer(mopt)
        logging.info("Magnet container started, "+repr(self.cont_conn))
        
    @defer.inlineCallbacks
    def _startCoreServices(self):
        yield bootstrap.bootstrap_core_services()
        logging.info("Core ION services started")

    @defer.inlineCallbacks
    def _stopMagnet(self):
        yield self.cont_conn.close(0)
        #yield self.cont_conn.delegate.close(None)
