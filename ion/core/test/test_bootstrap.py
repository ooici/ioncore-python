#!/usr/bin/env python

"""
@file ion/core/test/test_bootstrap.py
@author Michael Meisinger
@brief test case for bootstrapping the ION system
"""

import os

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core import bootstrap
from ion.core import ioninit
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class BootstrapTest1(IonTestCase):
    """ Tests the IonTestCase core classes. Starting container and services.
    """

    @defer.inlineCallbacks
    def test_start_container(self):
        yield self._start_container()

        log.info("Started capability container")

        yield self._stop_container()

class BootstrapTest2(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_1(self):
        pass

##  Potential tests for bootstrap_profile_dataset()  ## 
# @todo: test ion start and end time match the data in the time variable
# @todo: test that time variable is monotonic
# @todo: test that a supplement contains 2 new data values dimensioned by time
# @todo: test that the total number of values for the time dimension = (2 + overlap count)
# @todo: test invalid combinations of inputs for supplement_number, supplement_overlap_count and random_initialization
