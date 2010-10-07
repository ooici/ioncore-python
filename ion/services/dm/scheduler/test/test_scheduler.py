#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/test/test_scheduler.py
@date 9/21/10
@author Paul Hubbard
@test ion.services.dm.scheduler Exercise the crontab
"""

from twisted.internet import defer
from twisted.trial import unittest

from ion.core.process.service_process import ServiceProcess

from ion.services.dm.scheduler.scheduler_service import SchedulerServiceClient

from ion.test.iontest import IonTestCase
import ion.util.ionlog
from ion.util.procutils import asleep

log = ion.util.ionlog.getLogger(__name__)

class SchedulerTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 10
        services = [
            {'name': 'scheduler', 'module': 'ion.services.dm.scheduler.scheduler_service',
             'class': 'SchedulerService'},
            {'name' : 'attributestore', 'module' : 'ion.services.coi.attributestore',
             'class' : 'AttributeStoreService'},
            {'name' : 'scheduled_task', 'module' : 'ion.services.dm.scheduler.test.receiver',
             'class' : 'ScheduledTask'},
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
    def test_add_only(self):
        sc = SchedulerServiceClient(proc=self.sup)

        reply = yield sc.add_task('scheduled_task', 1.0, 'pingtest bar')
        task_id = reply['value']
        log.debug(task_id)
        self.failUnless(task_id != None)
        yield asleep(3.0)

    @defer.inlineCallbacks
    def test_add_remove(self):
        sc = SchedulerServiceClient(proc=self.sup)

        task_id = yield sc.add_task('scheduled_task', 1.0, 'pingtest foo')
        rc = yield sc.rm_task(task_id)
        self.failUnlessEqual(rc['value'], 'OK')
        log.debug(rc)
        yield asleep(3.0)

    @defer.inlineCallbacks
    def test_query(self):
        raise unittest.SkipTest('code not implemented yet')
        sc = SchedulerServiceClient(proc=self.sup)

        yield sc.add_task('foobar', 1.0, 'pingtest')
        rl = yield sc.query_tasks('.+?')
        self.failUnlessSubstring('foobar', str(rl['value']))

    @defer.inlineCallbacks
    def test_rm(self):
        raise unittest.SkipTest('code not implemented yet')
        sc = SchedulerServiceClient(proc=self.sup)

        yield sc.add_task('foobar', 1.0, 'pingtest')
        yield sc.rm_task('foobar')
        rl = yield sc.query_tasks('foobar')
        self.failUnlessEqual(rl['value'], [])
