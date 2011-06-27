#!/usr/bin/env python

"""
@file ion/core/test/test_bootstrap.py
@author Brian Fox
@brief specific test cases for ioninit module which are not covered otherwise
"""

import os

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.test.iontest import IonTestCase

class IoninitTest1(IonTestCase):
    """ Tests the IonTestCase core classes. Starting container and services.
    """

#    @defer.inlineCallbacks
#    def setUp(self):
#        yield self._start_container()

#    @defer.inlineCallbacks
#    def tearDown(self):
#        yield self._stop_container()

    def test_get_conf(self):
        conf = ioninit.get_config('ion.core.ioninit')

    def test_sys_log(self):
        import sys
        sys.platform = 'linux2'
        ioninit.use_syslog()
        
    def test_alt_logging(self):
        ioninit.use_alternate_logging('/tmp')