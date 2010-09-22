#!/usr/bin/env python

"""
@file ion/util/test/test_stat_object.py
@author Michael Meisinger
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks

from ion.util.state_object import StateObject, BasicStateObject, BasicFSMFactory
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class StateObjectTest(IonTestCase):
    """
    Tests state object stuff
    """

    def test_SO(self):
        so = TestSO()
        self._assertCounts(so, 0, 0, 0, 0, 0)
        so._so_process(BasicFSMFactory.E_INITIALIZE)
        self._assertCounts(so, 1, 0, 0, 0, 0)
        so._so_process(BasicFSMFactory.E_ACTIVATE)
        self._assertCounts(so, 1, 1, 0, 0, 0)
        so._so_process(BasicFSMFactory.E_DEACTIVATE)
        self._assertCounts(so, 1, 1, 1, 0, 0)
        so._so_process(BasicFSMFactory.E_ACTIVATE)
        self._assertCounts(so, 1, 2, 1, 0, 0)
        so._so_process(BasicFSMFactory.E_DEACTIVATE)
        self._assertCounts(so, 1, 2, 2, 0, 0)
        so._so_process(BasicFSMFactory.E_TERMINATE)
        self._assertCounts(so, 1, 2, 2, 1, 0)

        so._so_process(BasicFSMFactory.E_INITIALIZE)
        self._assertCounts(so, 1, 2, 2, 1, 1)
        so._so_process(BasicFSMFactory.E_ACTIVATE)
        self._assertCounts(so, 1, 2, 2, 1, 2)
        so._so_process(BasicFSMFactory.E_DEACTIVATE)
        self._assertCounts(so, 1, 2, 2, 1, 3)
        so._so_process(BasicFSMFactory.E_TERMINATE)
        self._assertCounts(so, 1, 2, 2, 1, 4)

        so = TestSO()
        so._so_process(BasicFSMFactory.E_ACTIVATE)
        self._assertCounts(so, 0, 0, 0, 0, 1)

        so = TestSO()
        so._so_process(BasicFSMFactory.E_DEACTIVATE)
        self._assertCounts(so, 0, 0, 0, 0, 1)

        so = TestSO()
        so._so_process(BasicFSMFactory.E_TERMINATE)
        self._assertCounts(so, 0, 0, 0, 0, 1)

    @defer.inlineCallbacks
    def test_SO_deferred(self):
        so = TestSODeferred()
        self._assertCounts(so, 0, 0, 0, 0, 0)

        yield so._so_process(BasicFSMFactory.E_INITIALIZE)
        self._assertCounts(so, 1, 0, 0, 0, 0)
        yield so._so_process(BasicFSMFactory.E_ACTIVATE)
        self._assertCounts(so, 1, 1, 0, 0, 0)

    def _assertCounts(self, so, init, act, deact, term, error):
        self.assertEqual(so.cnt_init, init)
        self.assertEqual(so.cnt_act, act)
        self.assertEqual(so.cnt_deact, deact)
        self.assertEqual(so.cnt_term, term)
        self.assertEqual(so.cnt_err, error)

class TestSO(BasicStateObject):
    def __init__(self):
        BasicStateObject.__init__(self)
        self.cnt_init = 0
        self.cnt_act = 0
        self.cnt_deact = 0
        self.cnt_term = 0
        self.cnt_err = 0

    def on_initialize(self, memory):
        self.cnt_init += 1
        log.debug("on_initialize called")

    def on_activate(self, memory):
        self.cnt_act += 1
        log.debug("on_activate called")

    def on_deactivate(self, memory):
        self.cnt_deact += 1
        log.debug("on_deactivate called")

    def on_terminate(self, memory):
        self.cnt_term += 1
        log.debug("on_terminate called")

    def on_error(self, memory):
        self.cnt_err += 1
        log.debug("on_error called")

class TestSODeferred(TestSO):
    def __init__(self):
        TestSO.__init__(self)

    @defer.inlineCallbacks
    def on_initialize(self, memory):
        TestSO.on_initialize(self, memory)
        log.debug("before sleep")
        yield pu.asleep(0.05)
        log.debug("done sleep")

    @defer.inlineCallbacks
    def on_activate(self, memory):
        TestSO.on_activate(self, memory)
        yield pu.asleep(0.05)

    @defer.inlineCallbacks
    def on_deactivate(self, memory):
        TestSO.on_deactivate(self, memory)
        yield pu.asleep(0.05)

    @defer.inlineCallbacks
    def on_terminate(self, memory):
        TestSO.on_terminate(self, memory)
        yield pu.asleep(0.05)

    @defer.inlineCallbacks
    def on_error(self, memory):
        TestSO.on_error(self, memory)
        yield pu.asleep(0.05)
