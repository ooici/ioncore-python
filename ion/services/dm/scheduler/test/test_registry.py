#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/test/test_registry.py
@date 9/24/10
@author Paul Hubbard
@test ion.services.dm.scheduler Exercise the crontab registry
"""

from twisted.internet import defer
from twisted.trial import unittest

from ion.services.dm.scheduler.scheduler_registry import SchedulerRegistryClient

from ion.test.iontest import IonTestCase

class SRT(IonTestCase):
    """
    Scheduler Registry test
    """
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 5
        services = [
            {'name': 'scheduler_registry', 'module': 'ion.services.dm.scheduler.scheduler_registry',
             'class': 'SchedulerRegistry'},
        ]

        yield self._start_container()
        self.sup = yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_service_init(self):
        # Just run the setup/teardown code
        pass

    @defer.inlineCallbacks
    def test_clear(self):
        sc = SchedulerRegistryClient(proc=self.sup)
        yield sc.clear()

    @defer.inlineCallbacks
    def test_add_remove(self):
        sc = SchedulerRegistryClient(proc=self.sup)
        tid = yield sc.store_task('foobar', 1.0)

        rc = yield sc.rm_task(tid)
        self.failUnlessEqual(rc['status'], 'OK')

    @defer.inlineCallbacks
    def test_query(self):
        raise unittest.SkipTest('query code incomplete')
        sc = SchedulerRegistryClient(proc=self.sup)

        yield sc.store_task('foobar', 1.0)
        rl = yield sc.query_tasks('.+?')
        self.failUnlessSubstring('foobar', str(rl['value']))

    @defer.inlineCallbacks
    def test_rm(self):
        raise unittest.SkipTest('rm not implemented in registry')
        sc = SchedulerRegistryClient(proc=self.sup)

        yield sc.store_task('foobar', 1.0)
        yield sc.rm_task('foobar')
        rl = yield sc.query_tasks('foobar')
        self.failUnlessEqual(rl['value'], [])
