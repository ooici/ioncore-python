#!/usr/bin/env python

"""
@file ion/util/test/test_task_chain.py
@author Dave Foster <dfoster@asascience.com>
"""

from twisted.trial import unittest
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.python import failure

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.test.iontest import IonTestCase
import ion.util.procutils as pu
from ion.util.task_chain import TaskChain
import os, os.path, sys

class TaskChainTest(IonTestCase):

    def setUp(self):
        self._def1 = defer.Deferred()
        self._def2 = defer.Deferred()
        self._def3 = defer.Deferred()

    def _call_one(self):
        self._def1.callback(True)
        return self._def1.result

    def _call_two(self, *args):
        self._def2.callback(args)
        return self._def2.result

    def _call_three(self, *args, **kwargs):
        self._def3.callback(kwargs)
        return self._def3.result

    def _err_two(self):
        ex = Exception("dyde")
        self._def2.errback(ex)

    @defer.inlineCallbacks
    def _deferred_two(self, *args):
        yield pu.asleep(1)
        self._def2.callback(args)
        defer.returnValue(self._def2.result)

    @defer.inlineCallbacks
    def test_task_len1(self):
        tc = TaskChain(self._call_one)
        res = yield tc.run()
        self.failUnlessIsInstance(res, list)
        self.failUnlessEquals(res, [(self._call_one, True)])
        self.failUnlessEquals(self._def1.result, True)

    @defer.inlineCallbacks
    def test_task_len2(self):
        tc = TaskChain(self._call_one, (self._call_two, ["starargs"]))
        res = yield tc.run()

        # for some reason this gets converted to a tuple from a list
        self.failUnlessEquals(res, [(self._call_one, True), (self._call_two, ("starargs",))])
        self.failUnlessEquals(self._def1.result, True)
        self.failUnlessEquals(self._def2.result, ("starargs",))

    @defer.inlineCallbacks
    def test_task_len3(self):
        tc = TaskChain(self._call_one, (self._call_two, ["starargs"]), (self._call_three, [], {"keyword":"args"}))
        res = yield tc.run()

        # for some reason this gets converted to a tuple from a list
        self.failUnlessEquals(res, [(self._call_one, True), (self._call_two, ("starargs",)), (self._call_three, {'keyword':'args'})])
        self.failUnlessEquals(self._def1.result, True)
        self.failUnlessEquals(self._def2.result, ("starargs",))
        self.failUnlessEquals(self._def3.result, {'keyword':'args'})

    def test_param_errors(self):
        """
        Test error checking of params added to chain
        """
        self.failUnlessRaises(ValueError, TaskChain, "string_instead_of_callable")
        self.failUnlessRaises(ValueError, TaskChain, (self._call_one, "string instead of list"))
        self.failUnlessRaises(ValueError, TaskChain, (self._call_one,))
        self.failUnlessRaises(ValueError, TaskChain, (self._call_one, [], "string instead of a dict"))
        self.failUnlessRaises(ValueError, TaskChain, (self._call_one, [], {}, "too long"))

    @defer.inlineCallbacks
    def test_err_in_chain(self):
        tc = TaskChain(self._call_one, self._err_two, (self._call_three, [], {}))
        res = yield tc.run()

        self.failUnlessIsInstance(self._def2.result, failure.Failure)

    @defer.inlineCallbacks
    def test_deferred_callable(self):
        tc = TaskChain(self._call_one, (self._deferred_two, ["deferred me!"]), (self._call_three, [], {}))
        res = yield tc.run()

        self.failUnlessEquals(res[1][1], ("deferred me!",))
        
