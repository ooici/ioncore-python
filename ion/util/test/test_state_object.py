#!/usr/bin/env python

"""
@file ion/util/test/test_stat_object.py
@author Michael Meisinger
"""

from twisted.trial import unittest
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.python import failure

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.util.state_object import StateObject, BasicLifecycleObject, BasicFSMFactory, BasicStates
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class StateObjectTest(IonTestCase):
    """
    Tests state object stuff
    """

    def test_SO(self):
        so = TestSO()
        self._assertCounts(so, 0, 0, 0, 0, 0)
        so._so_process(BasicStates.E_INITIALIZE)
        self._assertCounts(so, 1, 0, 0, 0, 0)
        so._so_process(BasicStates.E_ACTIVATE)
        self._assertCounts(so, 1, 1, 0, 0, 0)
        so._so_process(BasicStates.E_DEACTIVATE)
        self._assertCounts(so, 1, 1, 1, 0, 0)
        so._so_process(BasicStates.E_ACTIVATE)
        self._assertCounts(so, 1, 2, 1, 0, 0)
        so._so_process(BasicStates.E_DEACTIVATE)
        self._assertCounts(so, 1, 2, 2, 0, 0)
        so._so_process(BasicStates.E_TERMINATE)
        self._assertCounts(so, 1, 2, 2, 1, 0)

        # The following lead to errors
        so._so_process(BasicStates.E_INITIALIZE)
        self._assertCounts(so, 1, 2, 2, 1, 1)
        so._so_process(BasicStates.E_ACTIVATE)
        self._assertCounts(so, 1, 2, 2, 1, 2)
        so._so_process(BasicStates.E_DEACTIVATE)
        self._assertCounts(so, 1, 2, 2, 1, 3)
        so._so_process(BasicStates.E_TERMINATE)
        self._assertCounts(so, 1, 2, 2, 1, 4)

        so = TestSO()
        so._so_process(BasicStates.E_ACTIVATE)
        self._assertCounts(so, 0, 0, 0, 0, 1)

        so = TestSO()
        so._so_process(BasicStates.E_DEACTIVATE)
        self._assertCounts(so, 0, 0, 0, 0, 1)

        so = TestSO()
        so._so_process(BasicStates.E_TERMINATE)
        self._assertCounts(so, 0, 0, 0, 0, 1)

        so = TestSO()
        self._assertCounts(so, 0, 0, 0, 0, 0)
        res = so.initialize()
        self._assertCounts(so, 1, 0, 0, 0, 0)
        self.assertEqual(res, 33)
        so.activate()
        self._assertCounts(so, 1, 1, 0, 0, 0)
        so.deactivate()
        self._assertCounts(so, 1, 1, 1, 0, 0)

    @defer.inlineCallbacks
    def test_deferred_SO(self):
        so = TestSODeferred()
        self._assertCounts(so, 0, 0, 0, 0, 0)

        res = yield so._so_process(BasicStates.E_INITIALIZE)
        self._assertCounts(so, 1, 0, 0, 0, 0)
        self.assertEqual(res, 33)
        yield so._so_process(BasicStates.E_ACTIVATE)
        self._assertCounts(so, 1, 1, 0, 0, 0)

    def test_SO_error(self):
        # Tests error in state transition (not deferred) and error handler (not deferred)
        # Condition 1: error handler OK
        so = TestSO()
        self._assertCounts(so, 0, 0, 0, 0, 0)

        so.initialize()
        self._assertCounts(so, 1, 0, 0, 0, 0)
        try:
            so.activate(blow=True)
            self.fail("Exception expected")
        except RuntimeError, re:
            self.assertEqual(str(re),"blow")
        self._assertCounts(so, 1, 1, 0, 0, 1, 0)

        # Condition 2: error handler FAIL
        so = TestSO()
        so.initialize()
        try:
            so.activate(blow=True, errblow=True)
            self.fail("Exception expected")
        except RuntimeError, re:
            self.assertEqual(str(re),"errblow")
        self._assertCounts(so, 1, 1, 0, 0, 1, 1)

    @defer.inlineCallbacks
    def test_SO_def_error(self):
        # Tests error in state transition (not deferred) and error handler (deferred)
        # Condition 1: error handler OK
        so = TestSO_ED()

        self._assertCounts(so, 0, 0, 0, 0, 0)

        yield so.initialize()
        self._assertCounts(so, 1, 0, 0, 0, 0)
        try:
            yield so.activate(blow=True)
            self.fail("Exception expected")
        except RuntimeError, re:
            self.assertEqual(str(re),"blow")
        self._assertCounts(so, 1, 1, 0, 0, 1, 0)

        # Condition 2: error handler FAIL
        so = TestSO_ED()

        self._assertCounts(so, 0, 0, 0, 0, 0)

        yield so.initialize()
        self._assertCounts(so, 1, 0, 0, 0, 0)
        try:
            yield so.activate(blow=True, errblow=True)
            self.fail("Exception expected")
        except RuntimeError, re:
            self.assertEqual(str(re),"errblow")
        self._assertCounts(so, 1, 1, 0, 0, 1, 1)

    @defer.inlineCallbacks
    def test_deferred_SO_error(self):
        # Tests error in state transition (deferred) and error handler (not deferred)
        # Condition 1: error handler OK
        so = TestSODeferred_END()
        self._assertCounts(so, 0, 0, 0, 0, 0)

        yield so.initialize()
        self._assertCounts(so, 1, 0, 0, 0, 0)
        try:
            yield so.activate(blow=True)
            self.fail("Exception expected")
        except RuntimeError, re:
            self.assertEqual(str(re),"blow")
        self._assertCounts(so, 1, 1, 0, 0, 1, 0)

        # Condition 2: error handler FAIL
        so = TestSODeferred_END()
        self._assertCounts(so, 0, 0, 0, 0, 0)

        yield so.initialize()
        self._assertCounts(so, 1, 0, 0, 0, 0)
        try:
            yield so.activate(blow=True, errblow=True)
            self.fail("Exception expected")
        except RuntimeError, re:
            self.assertEqual(str(re),"errblow")
        self._assertCounts(so, 1, 1, 0, 0, 1, 1)

    @defer.inlineCallbacks
    def test_deferred_SO_def_error(self):
        # Tests error in state transition (deferred) and error handler (deferred)
        # Condition 1: error handler OK
        so = TestSODeferred()
        self._assertCounts(so, 0, 0, 0, 0, 0)

        yield so.initialize()
        self._assertCounts(so, 1, 0, 0, 0, 0)
        try:
            yield so.activate(blow=True)
            self.fail("Exception expected")
        except RuntimeError, re:
            self.assertEqual(str(re),"blow")
        self._assertCounts(so, 1, 1, 0, 0, 1, 0)

        # Condition 2: error handler FAIL
        so = TestSODeferred()
        self._assertCounts(so, 0, 0, 0, 0, 0)

        yield so.initialize()
        self._assertCounts(so, 1, 0, 0, 0, 0)
        try:
            yield so.activate(blow=True, errblow=True)
            self.fail("Exception expected")
        except RuntimeError, re:
            self.assertEqual(str(re),"errblow")
        self._assertCounts(so, 1, 1, 0, 0, 1, 1)

    def test_SO_argument(self):
        so = TestSO()
        so._so_process(BasicStates.E_INITIALIZE, 1, 2, 3)
        self._assertCounts(so, 1, 0, 0, 0, 0)
        self.assertEqual(so.args, (1, 2, 3))
        self.assertEqual(so.kwargs, {})
        so._so_process(BasicStates.E_ACTIVATE, a=1, b=2)
        self._assertCounts(so, 1, 1, 0, 0, 0)
        self.assertEqual(so.args, ())
        self.assertEqual(so.kwargs, dict(a=1, b=2))

        so = TestSO()
        so.initialize(1, 2, 3)
        self._assertCounts(so, 1, 0, 0, 0, 0)
        self.assertEqual(so.args, (1, 2, 3))
        self.assertEqual(so.kwargs, {})


    def _assertCounts(self, so, init, act, deact, term, error, errerr=0):
        self.assertEqual(so.cnt_init, init)
        self.assertEqual(so.cnt_act, act)
        self.assertEqual(so.cnt_deact, deact)
        self.assertEqual(so.cnt_term, term)
        self.assertEqual(so.cnt_err, error)
        self.assertEqual(so.cnt_errerr, errerr)

class TestSO(BasicLifecycleObject):
    def __init__(self):
        BasicLifecycleObject.__init__(self)
        self.cnt_init = 0
        self.cnt_act = 0
        self.cnt_deact = 0
        self.cnt_term = 0
        self.cnt_err = 0
        self.cnt_errerr = 0

    def on_initialize(self, *args, **kwargs):
        self.cnt_init += 1
        self.args = args
        self.kwargs = kwargs
        log.debug("on_initialize called")
        return 33

    def on_activate(self, *args, **kwargs):
        self.cnt_act += 1
        self.args = args
        self.kwargs = kwargs
        log.debug("on_activate called")
        if kwargs.get('errblow', False):
            raise RuntimeError("errblow")
        if kwargs.get('blow', False):
            raise RuntimeError("blow")

    def on_deactivate(self, *args, **kwargs):
        self.cnt_deact += 1
        self.args = args
        self.kwargs = kwargs
        log.debug("on_deactivate called")

    def on_terminate(self, *args, **kwargs):
        self.cnt_term += 1
        self.args = args
        self.kwargs = kwargs
        log.debug("on_terminate called")

    def on_error(self, *args, **kwargs):
        self.cnt_err += 1
        self.args = args
        self.kwargs = kwargs
        log.debug("on_error called")
        if len(args) == 0:
            # Case of illegal event error
            # @todo Distinguish
            return
        fail = args[0]
        if isinstance(fail, failure.Failure):
            fail = fail.getErrorMessage()
        if str(fail) == "errblow":
            self.cnt_errerr += 1
            raise RuntimeError("error error")

class TestSO_ED(TestSO):
    @defer.inlineCallbacks
    def on_error(self, *args, **kwargs):
        log.debug("deferred on_error called")
        TestSO.on_error(self, *args, **kwargs)
        yield pu.asleep(0.05)

class TestSODeferred(TestSO):
    def __init__(self):
        TestSO.__init__(self)

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        res = TestSO.on_initialize(self, *args, **kwargs)
        log.debug("before sleep")
        yield pu.asleep(0.05)
        log.debug("done sleep")
        defer.returnValue(res)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        yield pu.asleep(0.05)
        TestSO.on_activate(self, *args, **kwargs)
        yield pu.asleep(0.05)

    @defer.inlineCallbacks
    def on_deactivate(self, *args, **kwargs):
        TestSO.on_deactivate(self, *args, **kwargs)
        yield pu.asleep(0.05)

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        TestSO.on_terminate(self, *args, **kwargs)
        yield pu.asleep(0.05)

    @defer.inlineCallbacks
    def on_error(self, *args, **kwargs):
        TestSO.on_error(self, *args, **kwargs)
        log.debug("on_error() handler successful")
        yield pu.asleep(0.05)

class TestSODeferred_END(TestSODeferred):
    def on_error(self, *args, **kwargs):
        TestSO.on_error(self, *args, **kwargs)
