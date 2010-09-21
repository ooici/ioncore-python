#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/test/test_scheduler.py
@date 9/21/10
@author Paul Hubbard
@test ion.services.dm.scheduler Exercise the crontab
"""

from twisted.internet import defer

from ion.services.dm.scheduler.scheduler_service import SchedulerService, SchedulerServiceClient

from ion.test.iontest import IonTestCase

class SchedulerTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_service_init_only(self):

        services = [
            {'name': 'scheduler', 'module': 'ion.services.dm.scheduler.scheduler_service',
             'class': 'SchedulerService'},
        ]

        sup = yield self._spawn_processes(services)
