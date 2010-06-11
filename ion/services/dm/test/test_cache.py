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

class CacheTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 60

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_instantition_only(self):
        services = [
            {'name' : 'cache', 'module':'ion.services.dm.cache',
            'class' : 'CacheService'},
        ]
        sup = yield self._spawn_processes(services)
        cc = CacheClient(proc=sup)
