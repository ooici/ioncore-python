#!/usr/bin/env python

"""
@test ion.services.dm.test.test_cache.py
@author Paul Hubbard
@date 6/11/10
"""

from ion.services.dm.cache import CacheClient

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest
from ion.test.iontest import IonTestCase

import socket

class CacheTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 60
        services = [
            {'name' : 'cache', 'module':'ion.services.dm.cache',
            'class' : 'CacheService'},
        ]
        sup = yield self._spawn_processes(services)
        self.cc = CacheClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_instantition_only(self):
        pass

    @defer.inlineCallbacks
    def test_cache_hit(self):
        raise unittest.SkipTest('code not completed')

        das = yield self.cc.get_url('http://amoeba.ucsd.edu:8001/coads.nc.das')
        self.failUnlessSubstring('NC_GLOBAL', das)
