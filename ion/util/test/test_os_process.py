#!/usr/bin/env python

"""
@file ion/util/test/test_os_process.py
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
from ion.util.os_process import OSProcess, OSProcessError
import os, os.path, sys

class OSProcessTest(IonTestCase):

    def setUp(self):
        self._binls = None
        self._binecho = None
        self._binsleep = None
        self._usrbinenv = None

        if os.path.isfile("/bin/ls"):
            self._binls = "/bin/ls"

        if os.path.isfile("/bin/echo"):
            self._binecho = "/bin/echo"

        if os.path.isfile("/bin/sleep"):
            self._binsleep = "/bin/sleep"

        if os.path.isfile("/usr/bin/env"):
            self._usrbinenv = "/usr/bin/env"

        # don't check for env, just skip it below
        if self._binls is None or self._binsleep is None or self._binecho is None:
            raise unittest.SkipTest("No 'ls/echo/sleep' process found in path, is this windows or something?")

    @defer.inlineCallbacks
    def test_os_process_normal(self):
        osp = OSProcess(binary=self._binls)
        res = yield osp.spawn()
        self.failUnlessIsInstance(res, dict)
        self.failUnless(res.has_key("exitcode"))
        self.failUnlessEquals(res["exitcode"], 0)

    @defer.inlineCallbacks
    def test_os_process_error(self):
        osp = OSProcess(binary=self._binls, spawnargs=["--bogusoption"])
        err = yield self.failUnlessFailure(osp.spawn(), OSProcessError)
        self.failUnless(err.message["exitcode"] != 0)       # could be platform dependent, so just check non-0.  on osx it would be 1

    @defer.inlineCallbacks
    def test_os_process_env(self):
        if not self._usrbinenv:
            raise unittest.SkipTest("no /usr/bin/env available")

        osp = OSProcess(binary=self._usrbinenv, env={'test1': 'test1', 'which_test':'os_process_test'})
        res = yield osp.spawn()

        out = "\n".join(res["outlines"])
        self.failUnlessIn("test1", out)
        self.failUnlessIn("os_process_test", out)

    @defer.inlineCallbacks
    def test_os_process_close(self):
        osp = OSProcess(binary=self._binsleep, spawnargs=["10"])
        osp_def = osp.spawn()

        yield pu.asleep(1)

        res = yield osp.close()
        self.failUnlessIsInstance(res, dict)
        self.failUnlessEquals(res["exitcode"], 0)

    @defer.inlineCallbacks
    def test_os_process_close_force(self):
        # we can't really simulate a hung process but we can just tell it to die forcefully to get coverage+behavior testing
        osp = OSProcess(binary=self._binsleep, spawnargs=["10"])
        osp_def = osp.spawn()

        yield pu.asleep(1)

        res = yield osp.close(force=True)
        self.failUnlessIsInstance(res, dict)
        self.failUnlessEquals(res["exitcode"], 0)

    @defer.inlineCallbacks
    def test_os_process_output(self):
        osp = OSProcess(binary=self._binecho, spawnargs=["-n", "os process test"])
        res = yield osp.spawn()
        self.failUnlessEquals(res["outlines"][0], "os process test")

    @defer.inlineCallbacks
    def test_os_process_startdir(self):
        # we know /bin/ls, /bin/echo, /bin/lseep exist as we test in setup for it: dir it and find those items!
        osp = OSProcess(binary=self._binls, startdir="/bin")
        res = yield osp.spawn()

        out = "\n".join(res["outlines"])

        self.failUnlessIn("ls", out)
        self.failUnlessIn("echo", out)
        self.failUnlessIn("sleep", out)
