#!/usr/bin/env python

"""
@file ion/core/cc/test/test_cc_agent.py
@author Brian Fox
@brief test cases for cc_agent
"""

from ion.core import ioninit
from ion.test.iontest import IonTestCase
from ion.core.cc.cc_agent import CCAgent
import ion.util.procutils as pu

from twisted.internet import defer
from twisted.trial import unittest
from twisted.internet.task import LoopingCall

class CCAgentTest(IonTestCase):
    """
    Tests the module loader.
    """

    @defer.inlineCallbacks
    def setUp(self):
        self.completed = False;
        yield self._start_container()


    @defer.inlineCallbacks
    def tearDown(self):
        self.assertTrue(self.completed)
        yield self._stop_container()
        
    def test_cc_agent_init(self):
        cca = CCAgent()
        self.completed = True
        
    def test_cc_agent_plc_init(self):
        cca = CCAgent()
        cca.plc_init()
        self.completed = True

    @defer.inlineCallbacks
    def test_cc_agent_plc_activate(self):
        cca = CCAgent()
        cca.plc_init()
        yield cca.plc_activate()
        self.completed = True

    @defer.inlineCallbacks
    def test_cc_agent_plc_activate_with_announce(self):
        cca = CCAgent()
        cca.set_announce(True)
        cca.plc_init()
        yield cca.plc_activate()
        self.completed = True

    @defer.inlineCallbacks
    def test_cc_agent_plc_terminate(self):
        cca = CCAgent()
        cca.plc_init()
        yield cca.plc_activate()
        yield cca.plc_terminate()
        self.completed = True

    @defer.inlineCallbacks
    def test_cc_agent_plc_terminate_with_announce(self):
        cca = CCAgent()
        cca.set_announce(True)
        cca.plc_init()
        yield cca.plc_activate()
        yield cca.plc_terminate()
        self.completed = True
        
