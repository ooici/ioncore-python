#!/usr/bin/env python

"""
@file ion/core/test/test_baseprocess.py
@author Michael Meisinger
@brief test case for process base class
"""

import os
import logging

from twisted.trial import unittest
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn

from ion.core import ioninit
from ion.core.base_process import BaseProcess

from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class BaseProcessTest(IonTestCase):
    """
    Tests the process base classe.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._startContainer()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stopContainer()
        
    @defer.inlineCallbacks
    def test_process(self):
        p1 = BaseProcess()
        self.assertTrue(p1.receiver)
        
        rec = Receiver("myname")
        p2 = BaseProcess(rec)
        
        args = {'arg1':'value1','arg2':{}}
        p3 = BaseProcess(None, args)
        self.assertEquals(p3.spawnArgs, args)

        pid1 = yield p1.spawn()
        
        tp = EchoProcess()
        pid2 = yield tp.spawn()
        
        yield p1.rpc_send(pid2,'test','content')
        
    @defer.inlineCallbacks
    def _test_rpc(self):
        pass
    
class EchoProcess(BaseProcess):
    @defer.inlineCallbacks
    def op_test(self, content, headers, msg):
        logging.info("Message received: "+str(content))
        yield self.reply(msg,'result',content,{})
