#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/test/test_scheduler.py
@date 9/21/10
@author Paul Hubbard
@test ion.services.dm.scheduler Exercise the crontab
"""

from twisted.internet import defer

from ion.core.messaging.message_client import MessageClient
from ion.services.dm.scheduler.scheduler_service import SchedulerServiceClient
from ion.services.dm.scheduler.test.receiver import STClient

from ion.test.iontest import IonTestCase
import ion.util.ionlog
from ion.util.procutils import asleep

log = ion.util.ionlog.getLogger(__name__)


ADDTASK_REQ_TYPE     = object_utils.create_type_identifier(object_id=2601, version=1)
ADDTASK_RSP_TYPE     = object_utils.create_type_identifier(object_id=2602, version=1)
RMTASK_REQ_TYPE      = object_utils.create_type_identifier(object_id=2603, version=1)
RMTASK_RSP_TYPE      = object_utils.create_type_identifier(object_id=2604, version=1)
QUERYTASK_REQ_TYPE   = object_utils.create_type_identifier(object_id=2605, version=1)
QUERYTASK_RSP_TYPE   = object_utils.create_type_identifier(object_id=2606, version=1)


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
    def _add(self, origin, interval, payload):
        mc = MessageClient(proc=self.sup)

        msg_a = yield self.mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.desired_origin    = self.dest
        msg_a.interval_seconds  = 3
        msg_a.payload           = 'pingtest bar'

        return yield sc.add_task(msg_a)
    
    @defer.inlineCallbacks
    def _rm(self, task_id):
        mc = MessageClient(proc=self.sup)

        msg_r = yield self.mc.create_instance(RMTASK_REQ_TYPE)
        msg_r.task_id = task_id

        return yield sc.rm_task(msg_r)
    
    @defer.inlineCallbacks
    def _query(self, regex):
        mc = MessageClient(proc=self.sup)

        msg_q = yield self.mc.create_instance(QUERYTASK_REQ_TYPE)
        msg_q.task_regex = regex

        return yield self.query_task(msg_q)

    @defer.inlineCallbacks
    def test_complete_usecase(self):
        """
        Add a task, get a message, remove same.
        """
        # Create clients
        mc = MessageClient(proc=self.sup)
        sc = SchedulerServiceClient(proc=self.sup)

        resp_msg = self._add(self.dest, 3, 'pingtest_bar')

        log.debug(resp_msg.task_id)
        self.failIf(resp_msg.task_id is None)
        #fixme: also fail if we don't get GPB #2602 back

        # Wait for a message to go through the system
        yield asleep(5)
        mc = yield self.client.get_count()
        self.failUnless(int(mc['value']) >= 1)

        rc = self._rm(resp_msg.task_id)
        self.failUnlessEqual(rc.value, 'OK')
        yield asleep(0.5)

    @defer.inlineCallbacks
    def test_add_remove(self):
        # Create clients
        mc = MessageClient(proc=self.sup)
        sc = SchedulerServiceClient(proc=self.sup)

        resp_msg = self._add(self.dest, 10, 'pingtest_foo')

        rc = self._rm(resp_msg.task_id)
        self.failUnlessEqual(rc.value, 'OK')
        log.debug(rc)

    @defer.inlineCallbacks
    def test_query(self):
        # Create clients
        mc = MessageClient(proc=self.sup)
        sc = SchedulerServiceClient(proc=self.sup)

        self._add(self.dest, 1, 'baz')

        msg_q = yield slef.mc.create_instance(QUERYTASK_REQ_TYPE)
        msg_q.task_regex = '.+'
        rl = yield sc.query_tasks(msg_q)

        #FIXME... also, why is this equal to 2 and not 1?
        self.failUnless(len(rl['value']) == 2)
        self.failUnlessSubstring(str(task_id), str(rl['value']))

    @defer.inlineCallbacks
    def test_rm(self):
        # Create clients
        mc = MessageClient(proc=self.sup)
        sc = SchedulerServiceClient(proc=self.sup)

        resp_msg = self._add(self.dest, 1, 'pingtest')

        yield sc.rm_task(resp_msg.task_id)
        
        rl = yield sc.query_tasks(task_id)
        
        log.debug(rl)
        self.failUnlessEqual(len(rl['value']), 0)
        yield asleep(0.5)
