#!/usr/bin/env python

"""
@file ion/util/test/test_procutils.py
@author David Stuebe
"""
import os

from twisted.trial import unittest
from twisted.internet import defer

from ion.util import procutils as pu
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


class ProcUtilsTest(unittest.TestCase):

    def test_capture_stdout(self):


        def myfunc(arg1,arg2,kwarg1=None,kwarg2=None):

            print 'This is my Function:'
            print 'My Args are: "%s", "%s"' % (arg1, arg2)
            print 'My Kwargs are: "%s", "%s"' % (kwarg1,kwarg2)

            return

        val = pu.capture_function_stdout(myfunc,'1','2',kwarg1=1, kwarg2=2)

        res = \
"""This is my Function:
My Args are: "1", "2"
My Kwargs are: "1", "2"
"""

        print "Stdout is back to normal"


        self.assertEqual(res,val)

    @defer.inlineCallbacks
    def test_print_memory_usage(self):
        #os.putenv("ION_TEST_CASE_PIDS","5925")
        output = yield pu.print_memory_usage()
        log.info(output)
        