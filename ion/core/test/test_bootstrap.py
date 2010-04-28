#!/usr/bin/env python

"""
@file ion/core/test/test_bootstrap.py
@author Michael Meisinger
@brief test case for bootstrapping the ION system
"""

import os
import logging

from twisted.application.service import ServiceMaker

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, DeferredQueue

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.core import bootstrap
from ion.core import ioninit
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class BootstrapTest(IonTestCase):
    
            
    @defer.inlineCallbacks
    def setUp(self):
        IonTestCase.setUp(self)
        yield self._startMagnet()
        yield self._startCoreServices()

    @defer.inlineCallbacks
    def tearDown(self):
        IonTestCase.tearDown(self)
        yield self._stopMagnet()

    @defer.inlineCallbacks
    def _test_1(self):
        receiver = Receiver(__name__)
        def receive(content, msg):
            print 'in receive ', content, msg
        receiver.handle(receive)
        id = yield spawn(receiver)
        yield send(id.full, {'key':'obj1','value':'999'})

    @defer.inlineCallbacks
    def test_2(self):
        pass
        #yield bootstrap.start()


