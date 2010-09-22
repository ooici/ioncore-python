#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/test/test_scheduler.py
@date 9/21/10
@author Paul Hubbard
@test ion.services.dm.scheduler Exercise the crontab
"""

from twisted.internet import defer

from ion.services.dm.scheduler.scheduler_service import SchedulerServiceClient

from ion.test.iontest import IonTestCase

class SchedulerTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 5
        services = [
            {'name': 'scheduler', 'module': 'ion.services.dm.scheduler.scheduler_service',
             'class': 'SchedulerService'},
        ]

        yield self._start_container()
        self.sup = yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_service_init_only(self):
        pass

    @defer.inlineCallbacks
    def test_add_remove(self):
        sc = SchedulerServiceClient(proc=self.sup)

        yield sc.add_task('foobar', 1.0, 'pingtest')
        rc = yield sc.rm_task('foobar')
        self.failUnlessEqual(rc['status'], 'OK')

    @defer.inlineCallbacks
    def test_query(self):
        sc = SchedulerServiceClient(proc=self.sup)

        yield sc.add_task('foobar', 1.0, 'pingtest')
        rl = yield sc.query_tasks('.+?')
        self.failUnlessSubstring('foobar', str(rl['value']))

    @defer.inlineCallbacks
    def test_rm(self):
        sc = SchedulerServiceClient(proc=self.sup)

        yield sc.add_task('foobar', 1.0, 'pingtest')
        yield sc.rm_task('foobar')
        rl = yield sc.query_tasks('foobar')
        self.failUnlessEqual(rl['value'], [])
