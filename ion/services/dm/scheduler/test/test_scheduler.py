#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/test/test_scheduler.py
@date 9/21/10
@author Paul Hubbard
@test ion.services.dm.scheduler Exercise the crontab
"""
import time

from twisted.internet import defer
from ion.core.exception import ReceivedApplicationError

from ion.core.process.process import Process
from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.services.dm.distribution.events import ScheduleEventSubscriber
from ion.services.dm.scheduler.scheduler_service import SchedulerServiceClient

from ion.test.iontest import IonTestCase
import ion.util.ionlog
from ion.util.iontime import IonTime
from ion.util.procutils import asleep

log = ion.util.ionlog.getLogger(__name__)


# get configuration
from ion.core import ioninit
CONF = ioninit.config(__name__)

ADDTASK_REQ_TYPE     = object_utils.create_type_identifier(object_id=2601, version=1)
ADDTASK_RSP_TYPE     = object_utils.create_type_identifier(object_id=2602, version=1)
RMTASK_REQ_TYPE      = object_utils.create_type_identifier(object_id=2603, version=1)
RMTASK_RSP_TYPE      = object_utils.create_type_identifier(object_id=2604, version=1)
QUERYTASK_REQ_TYPE   = object_utils.create_type_identifier(object_id=2605, version=1)
QUERYTASK_RSP_TYPE   = object_utils.create_type_identifier(object_id=2606, version=1)

# other messages used for payloads
SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE_PAYLOAD_TYPE = object_utils.create_type_identifier(object_id=2607, version=1)

# desired_origins
from ion.services.dm.scheduler.scheduler_service import SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE



class SchedulerTest(IonTestCase):


    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 10

        services = [
            {'name': 'scheduler', 'module': 'ion.services.dm.scheduler.scheduler_service',
             'class': 'SchedulerService',
             'spawnargs': self._get_spawn_args()},
        ]

        yield self._start_container()

        yield self._setup_store()
        yield self._spawn_processes(services)

        self.proc = Process(spawnargs={'proc-name':'SchedulerTestProcess'})
        yield self.proc.spawn()

        # Pretend like everything in the Scheduler test process is happening in response to a message...
        self.proc.context = self.proc.conversation_context.create_context('dont delete me!')

        # setup subscriber for trigger event
        self._notices = []
        self.sub = ScheduleEventSubscriber(process=self.proc,
                                           origin=SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE)

        # you can not keep the received message around after the ondata callback is complete
        self.sub.ondata = lambda c: self._notices.append(c['content'].additional_data.payload.dataset_id)

        # normally we'd register before initialize/activate but let's not bring the PSC/EMS into the mix
        # if we can avoid it.
        yield self.sub.initialize()
        yield self.sub.activate()

    def _get_spawn_args(self):
        """
        Override this in derived tests for Cassandra setup for services, etc.
        """
        return {}

    @defer.inlineCallbacks
    def _setup_store(self):
        """
        Override this in derived tests for Cassandra setup, etc.
        """
        yield 1
        defer.returnValue(None)

    @defer.inlineCallbacks
    def tearDown(self):

        yield self._cleanup_store()
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def _cleanup_store(self):
        """
        Override this in derived tests for Cassandra cleanup, etc.
        """
        yield 1
        defer.returnValue(None)

    def test_service_init(self):
        # Just run the setup/teardown code
        pass

    @defer.inlineCallbacks
    def test_add_remove(self):
        # Create clients
        mc = MessageClient(proc=self.proc)
        sc = SchedulerServiceClient(proc=self.proc)

        msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_a.interval_seconds  = 10
        msg_a.payload           = msg_a.CreateObject(SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE_PAYLOAD_TYPE)
        msg_a.payload.dataset_id = "TESTER"
        msg_a.payload.datasource_id = "TWO"

        resp_msg = yield sc.add_task(msg_a)

        msg_r = yield mc.create_instance(RMTASK_REQ_TYPE)
        msg_r.task_id = resp_msg.task_id

        rc = yield sc.rm_task(msg_r)

        self.failUnlessEqual(rc.value, 'OK')
        #log.debug(rc)

    @defer.inlineCallbacks
    def test_add_remove_duplicate_task_id(self):
        """
        Making sure we can specify a "known" task_id and that it won't duplicate.
        """
        # Create clients
        mc = MessageClient(proc=self.proc)
        sc = SchedulerServiceClient(proc=self.proc)

        task_id = "the_one_true_task"

        msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.task_id           = task_id
        msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_a.interval_seconds  = 10
        msg_a.payload           = msg_a.CreateObject(SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE_PAYLOAD_TYPE)
        msg_a.payload.dataset_id = "TESTER"
        msg_a.payload.datasource_id = "TWO"

        resp_msg = yield sc.add_task(msg_a)
        self.failIf(resp_msg.duplicate)

        # try to schedule it again!
        msg_b = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_b.task_id           = task_id
        msg_b.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_b.interval_seconds  = 30
        msg_b.payload           = msg_a.CreateObject(SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE_PAYLOAD_TYPE)
        msg_b.payload.dataset_id = "SOME SIMILAR TASK"
        msg_b.payload.datasource_id = "BUT NOT RLY"

        msg_r = yield mc.create_instance(RMTASK_REQ_TYPE)
        msg_r.task_id = task_id

        resp_msg = yield sc.add_task(msg_b)
        self.failUnless(resp_msg.duplicate)

        rc = yield sc.rm_task(msg_r)
        self.failUnlessEqual(rc.value, 'OK')

    @defer.inlineCallbacks
    def test_complete_usecase(self):
        """
        Add a task, get a message, remove same.
        """
        # Create clients
        sc = SchedulerServiceClient(proc=self.proc)
        mc = self.proc.message_client

        msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_a.interval_seconds  = 1
        msg_a.payload           = msg_a.CreateObject(SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE_PAYLOAD_TYPE)
        msg_a.payload.dataset_id = "TESTER"
        msg_a.payload.datasource_id = "TWO"

        resp_msg = yield sc.add_task(msg_a)

        log.debug(resp_msg.task_id)
        self.failIf(resp_msg.task_id is None)

        #fixme: also fail if we don't get GPB #2602 back

        # Wait for a message to go through the system
        yield asleep(3)
        #cc = yield self.client.get_count()
        #self.failUnless(int(cc['value']) >= 1)
        self.failUnless(len(self._notices) > 1, "this may fail intermittently due to messaging")
        #self.failUnlessEquals(self._notices[0]['content'].additional_data.payload.dataset_id, "TESTER")
        #self.failUnlessEquals(self._notices[0]['content'].additional_data.payload.datasource_id, "TWO")
        self.failUnlessEquals(self._notices[0], "TESTER")


        msg_r = yield mc.create_instance(RMTASK_REQ_TYPE)
        msg_r.task_id = resp_msg.task_id

        rc = yield sc.rm_task(msg_r)

        self.failUnlessEqual(rc.value, 'OK')
        yield asleep(0.5)

    @defer.inlineCallbacks
    def test_rm(self):
        # Create clients
        mc = MessageClient(proc=self.proc)
        sc = SchedulerServiceClient(proc=self.proc)

        msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_a.interval_seconds  = 1
        msg_a.payload           = msg_a.CreateObject(SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE_PAYLOAD_TYPE)
        msg_a.payload.dataset_id = "TESTER"
        msg_a.payload.datasource_id = "TWO"

        resp_msg = yield sc.add_task(msg_a)

        msg_r = yield mc.create_instance(RMTASK_REQ_TYPE)
        msg_r.task_id = resp_msg.task_id

        yield sc.rm_task(msg_r)
        

    @defer.inlineCallbacks
    def test_future_start_time(self):

        mc = MessageClient(proc=self.proc)
        sc = SchedulerServiceClient(proc=self.proc)

        msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_a.interval_seconds  = 1

        msg_a.payload           = msg_a.CreateObject(SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE_PAYLOAD_TYPE)
        msg_a.payload.dataset_id = "THE FUTURE"
        msg_a.payload.datasource_id = "IS NOW"

        # calc a start time 5 sec in the future
        starttime = IonTime().time_ms + 5000
        msg_a.start_time        = starttime

        yield sc.add_task(msg_a)

        # sleep for 4 seconds: should see nothing
        yield asleep(4)
        self.failUnlessEquals(len(self._notices), 0)

        # sleep for another 3 - should give us enough time to get 1
        yield asleep(3)
        self.failUnless(len(self._notices) > 0, "Could be an intermittent failure, waiting for message delivery")

    @defer.inlineCallbacks
    def test_past_start_time(self):

        mc = MessageClient(proc=self.proc)
        sc = SchedulerServiceClient(proc=self.proc)

        msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_a.interval_seconds  = 30

        msg_a.payload           = msg_a.CreateObject(SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE_PAYLOAD_TYPE)
        msg_a.payload.dataset_id = "THE PAST"
        msg_a.payload.datasource_id = "IS A WHILE AGO"

        # calc a start time 25 sec in the past
        starttime = IonTime().time_ms - 25000
        msg_a.start_time        = starttime

        yield sc.add_task(msg_a)

        # sleep for 2 seconds, no messages yet
        yield asleep(2)
        self.failUnlessEquals(len(self._notices), 0)

        yield asleep(4)
        self.failUnless(len(self._notices) > 0, "Could be an intermittent failure, waiting for message delivery")

    @defer.inlineCallbacks
    def test_invalid_input(self):

        mc = MessageClient(proc=self.proc)
        sc = SchedulerServiceClient(proc=self.proc)

        msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_a.interval_seconds  = 30

        # Platform differences between my machine and buildbot make it so that I can't actually generate this number
        # this test is not as important anyway, the +1 year check is more important and will catch the same thing.
        
        ## calc a time, multiply by 1000 again to make it woefully out of range
        #starttime = IonTime().time_ms * 1000
        #msg_a.start_time        = starttime

        ## send it, expect an error!
        #scdef = sc.add_task(msg_a)
        #yield self.failUnlessFailure(scdef, ReceivedApplicationError)
        #self.failUnlessEquals(scdef.result.msg_content.MessageResponseCode, scdef.result.msg_content.ResponseCodes.BAD_REQUEST)

        ## now try it with scheduling a start time that is ok, but start_time plus interval is not
        #msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        #msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        #msg_a.interval_seconds  = 30

        ## use the maximum integer, convert to ms
        #msg_a.start_time        = sys.maxint * 1000

        ## send it, expect an error!
        #scdef = sc.add_task(msg_a)
        #yield self.failUnlessFailure(scdef, ReceivedApplicationError)
        #self.failUnlessEquals(scdef.result.msg_content.MessageResponseCode, scdef.result.msg_content.ResponseCodes.BAD_REQUEST)

        # now try it with scheduling a start time more than a year from today
        msg_a = yield mc.create_instance(ADDTASK_REQ_TYPE)
        msg_a.desired_origin    = SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE
        msg_a.interval_seconds  = 30

        # use the maximum integer, convert to ms
        msg_a.start_time        = (int(time.time()) + 31536000 + 30) * 1000

        # send it, expect an error!
        scdef = sc.add_task(msg_a)
        yield self.failUnlessFailure(scdef, ReceivedApplicationError)
        self.failUnlessEquals(scdef.result.msg_content.MessageResponseCode, scdef.result.msg_content.ResponseCodes.BAD_REQUEST)
