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

from magnet import container
from magnet.service import Magnet
from magnet.service import Options

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.core import ioninit
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class BootstrapTest(IonTestCase):
    
    @defer.inlineCallbacks
    def _startMagnet(self):
        mopt = {}
        mopt['broker_host'] = 'amoeba.ucsd.edu'
        mopt['broker_port'] = 5672
        mopt['broker_vhost'] = '/'
        mopt['boot_script'] = None
        mopt['script'] = None
 
        self.conn = yield container.startContainer(mopt)
        print "started, ", repr(self.conn)

        #script = os.path.abspath(mopt['script'])
        #print "script ", script
        #if os.path.isfile(script):
        #    print "Executing script %s ..." % mopt['script']
        #    execfile(script, {})
        #    
            
    @defer.inlineCallbacks
    def setUp(self):
        IonTestCase.setUp(self)
        yield self._startMagnet()

    @defer.inlineCallbacks
    def tearDown(self):
        IonTestCase.tearDown(self)
        yield self.conn.close(0)
        #yield self.conn.delegate.close(None)

    @defer.inlineCallbacks
    def test_1(self):
        receiver = Receiver(__name__)
        def receive(content, msg):
            print 'in receive ', content, msg
        receiver.handle(receive)
        id = yield spawn(receiver)
        yield send(id.full, {'key':'obj1','value':'999'})

