#!/usr/bin/env python

"""
@file ion/test/loadtest.py
@author Michael Meisinger
@brief Base class for a load test
"""

from twisted.internet import defer

#from ion.test.iontest import IonTestCase

#import ion.util.ionlog
#log = ion.util.ionlog.getLogger(__name__)

class LoadTestSuite(object):
    pass

class LoadTest(object):
    """
    @note careful. If a unittest, will be automatically be picked up by trial
    """

    def setUp(self):
        "Hook method for setting up the test fixture before exercising it."
        return defer.succeed(None)

    def tearDown(self):
        "Hook method for deconstructing the test fixture after testing it."
        return defer.succeed(None)

    def generate_load(self, *args):
        return defer.succeed(None)
