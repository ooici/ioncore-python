#!/usr/bin/env python

"""
@file ion/core/test/test_bootstrap.py
@author Michael Meisinger
@brief test case for bootstrapping the ION system
"""

import os
import logging
log = logging.getLogger(__name__)

from twisted.application.service import ServiceMaker

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, DeferredQueue

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from ion.data.store import Store

from ion.core import bootstrap
from ion.core import ioninit
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class BootstrapTest1(IonTestCase):
    """ Tests the IonTestCase core classes. Starting container and services.
    """

    @defer.inlineCallbacks
    def test_start_container(self):
        yield self._start_container()

        log.info("Started magnet container")

        yield self._stop_container()

    @defer.inlineCallbacks
    def test_startContainerAndServices(self):
        yield self._start_container()
        yield self._start_core_services()

        log.info("Started magnet and core services")

        yield self._stop_container()



class BootstrapTest2(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_1(self):
        pass
