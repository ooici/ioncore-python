#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/test/test_scheduler.py
@date 9/21/10
@author Paul Hubbard
@test ion.services.dm.scheduler Exercise the crontab
"""

from twisted.internet import defer

from ion.services.dm.scheduler.scheduler_service import SchedulerServiceClient
from ion.services.dm.scheduler.test.receiver import STClient

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

        # Look up the address of the test receiver by name
        sptid = yield self._get_procid('scheduled_task')
        self.dest = str(sptid)
        # Instantiate the process client (receiver)
        self.client = STClient(target=sptid)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_service_init(self):
        # Just run the setup/teardown code
        pass

    @defer.inlineCallbacks
    def test_complete_usecase(self):
        """
        Add a task, get a message, remove same.
        """
        sc = SchedulerServiceClient(proc=self.sup)

        reply = yield sc.add_task(self.dest, 0.3, 'pingtest bar')
        task_id = reply['value']
        log.debug(task_id)
        self.failIf(task_id == None)

        # Wait for a message to go through the system
        yield asleep(0.5)
        mc = yield self.client.get_count()
        self.failUnless(int(mc['value']) >= 1)

        rc = yield sc.rm_task(task_id)
        self.failUnlessEqual(rc['value'], 'OK')

    @defer.inlineCallbacks
    def test_add_remove(self):
        sc = SchedulerServiceClient(proc=self.sup)

        task_id = yield sc.add_task(self.dest, 10.5, 'pingtest foo')
        rc = yield sc.rm_task(task_id)
        self.failUnlessEqual(rc['value'], 'OK')
        log.debug(rc)

    @defer.inlineCallbacks
    def test_query(self):
        sc = SchedulerServiceClient(proc=self.sup)

        yield sc.add_task(self.dest, 0.5, 'baz')
        reply = yield sc.add_task('scheduled_task', 1.0, 'pingtest')
        task_id = reply['value']
        rl = yield sc.query_tasks('.+')
        self.failUnless(len(rl) == 2)
        self.failUnlessSubstring(str(task_id), str(rl['value']))

    @defer.inlineCallbacks
    def test_rm(self):
        sc = SchedulerServiceClient(proc=self.sup)

        reply = yield sc.add_task(self.dest, 1.0, 'pingtest')
        task_id = reply['value']
        yield sc.rm_task(task_id)
        rl = yield sc.query_tasks(task_id)
        log.debug(rl)
        self.failUnlessEqual(len(rl['value']), 0)
