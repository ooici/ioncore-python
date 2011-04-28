#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/scheduler_service.py
@date 9/21/10
@author Paul Hubbard
@package ion.services.dm.scheduler.service Implementation of the scheduler
"""
import time
from ion.core.data.store import IndexStore, Query
from ion.core.exception import ApplicationError
from ion.core.object.gpb_wrapper import StructureElement
from ion.core.object.repository import ObjectContainer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor
import re
from uuid import uuid4

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.attributestore import AttributeStoreClient
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils
from ion.services.dm.distribution.events import TriggerEventPublisher, ScheduleEventPublisher

# constants from https://confluence.oceanobservatories.org/display/syseng/Scheduler+Events
# import these and use them to schedule your events, they should be in the "desired origin" field
SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE="1001"

ADDTASK_REQ_TYPE  = object_utils.create_type_identifier(object_id=2601, version=1)
"""
message AddTaskRequest {
    enum _MessageTypeIdentifier {
      _ID = 2601;
      _VERSION = 1;
    }

    // desired_origin is where the event notification will originate from
    //   this is not required to be sent... one will be generated if not
    // interval is seconds between messages
    // payload is string

    optional string desired_origin    = 1;
    optional uint64 interval_seconds  = 2;
    optional string payload           = 3;

    //these are actually optional: epoch times for start/end
    //optional uint64 time_start_unix   = 4;
    //optional uint64 time_end_unix     = 5;
}
"""

ADDTASK_RSP_TYPE  = object_utils.create_type_identifier(object_id=2602, version=1)
"""
message AddTaskResponse {
    enum _MessageTypeIdentifier {
      _ID = 2602;
      _VERSION = 1;
    }

    // the string guid
    // the origin  is where the event notifications will come from

    optional string task_id = 1;
    optional string origin  = 2;
}

"""


RMTASK_REQ_TYPE   = object_utils.create_type_identifier(object_id=2603, version=1)
"""
message RmTaskRequest {
    enum _MessageTypeIdentifier {
      _ID = 2603;
      _VERSION = 1;
    }

    // task id is GUID
    optional string task_id = 1;

}
"""

RMTASK_RSP_TYPE   = object_utils.create_type_identifier(object_id=2604, version=1)


QUERYTASK_REQ_TYPE   = object_utils.create_type_identifier(object_id=2605, version=1)
"""
message QueryTaskRequest {
    enum _MessageTypeIdentifier {
      _ID = 2605;
      _VERSION = 1;
    }

    optional string task_regex = 1;

}
"""

QUERYTASK_RSP_TYPE   = object_utils.create_type_identifier(object_id=2606, version=1)
"""
message QueryTaskResponse {
    enum _MessageTypeIdentifier {
      _ID = 2606;
      _VERSION = 1;
    }

    // can be an empty list
    repeated string task_ids = 1;

}
"""

class SchedulerError(ApplicationError):
    """
    Raised when invalid params are passed to an op on the scheduler.
    """
    pass


class SchedulerService(ServiceProcess):
    """
    First pass at a message-based cron service, where you register a send-to address,
    interval and payload, and the scheduler will message you when the timer expires.
    @note this will be subsumed into CEI at some point; consider this a prototype.
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='scheduler',
                                          version='0.1.1',
                                          dependencies=['attributestore'])

    class SchedulerIndexStore(IndexStore):
        """
        Specifically derived IndexStore for scheduler use.
        We do NOT want to use class variables for storage, we want fresh copies
        on every instance.
        """
        def __init__(self, *args, **kwargs):
            self.kvs = {}
            self.indices = {}

            IndexStore.__init__(self, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        ServiceProcess.__init__(self, *args, **kwargs)

        # columns we have
        indices = ['task_id',
                   'desired_origin',
                   'interval_seconds',
                   'payload',
                   'user_id',
                   'constant',
                   'start_time',
                   'end_time']

        self.scheduled_events = self.SchedulerIndexStore(indices=indices)
        self.mc = MessageClient(proc=self)
        self.pub = ScheduleEventPublisher(process=self)

        # maps task_ids to IDelayedCall objects, popped off when callback is called, used to cancel tasks
        self._callback_tasks = {}

        # will move pub through the lifecycle states with the service
        self.add_life_cycle_object(self.pub)

    @defer.inlineCallbacks
    def slc_activate(self):
        # get all items from the store
        query = Query()
        query.add_predicate_eq('constant', '1')
        rows = yield self.scheduled_events.query(query)

        for task_id, tdef in rows.iteritems():
            log.debug("slc_activate: scheduling %s" % task_id)
            self._schedule_event(tdef['start_time'], tdef['interval_seconds'], task_id)

    def slc_terminate(self):
        """
        Called before terminate, this is a good place to tear down the AS and jobs.
        @todo iterate over the list
        foreach task in op_query:
          rm_task(task)
        """
        for k, v in self._callback_tasks.iteritems():
            v.cancel()

    def _schedule_event(self, starttime, interval, task_id):
        """
        Helper method to schedule and record a callback in the service.
        Used by op_add_task and on startup.

        @param  starttime   The time to start the callbacks. This is used with the interval to calculate the
                            first callback. If None is specified, will use now. Note: the first callback to
                            occur will not happen immediatly, it will be after the first interval has elapsed,
                            whether starttime is specified or not. This parameter should be specified in UNIX
                            epoch format, in ms. You will have to conver the output from time.time() in Python.
        @param  interval    The interval to trigger scheduler events, in seconds.
        @param  task_id     The task_id to trigger.
        """
        assert interval and task_id and interval > 0
        curtime = int(round(time.time() * 1000))
        starttime = starttime or curtime

        # determine first callback time
        diff = curtime - starttime
        if diff > 0:
            # we started a while ago, so just find what is remaining of the interval from now
            lefttimems = diff % (interval * 1000)
            calctime = interval - int(lefttimems / 1000)
        else:
            # start time is in THE FUTURE
            calctime = 0 - int(diff/1000) + interval

        log.debug("_schedule_event: calculated next callback time of %d" % calctime)

        ccl = reactor.callLater(calctime, self._send_and_reschedule, task_id)
        self._callback_tasks[task_id] = ccl

    @defer.inlineCallbacks
    def op_add_task(self, content, headers, msg):
        """
        @brief Add a new task to the crontab. Interval is in seconds.
        @param content Message payload, must be a GPB #2601
        @param headers Ignored here
        @param msg Ignored here
        @retval reply_ok or reply_err
        """
        try:
            task_id         = str(uuid4())
            msg_interval    = content.interval_seconds
            desired_origin  = content.desired_origin
            if content.IsFieldSet('start_time'):
                starttime = content.start_time
            else:
                starttime = None
            if content.IsFieldSet('payload'):
                # extract, serialize
                payload = content.Repository.index_hash[content.payload.MyId].serialize()
            else:
                payload = None
            if content.IsFieldSet('end_time'):
                log.warn("Scheduler does not handle end_time yet!")
                endtime = content.end_time
            else:
                endtime = None
            if content.IsFieldSet('user_id'):
                user_id = content.user_id
            else:
                user_id = ''

        except KeyError, ke:
            log.exception('Required keys in op_add_task content not found!')
            raise SchedulerError(str(ke))

        log.debug('ok, gotta task to save')

        #create the response: task_id and actual origin
        resp            = yield self.mc.create_instance(ADDTASK_RSP_TYPE)
        resp.task_id    = task_id
        resp.origin     = desired_origin

        # extract content of message
        self.scheduled_events.put(task_id,
                                  task_id,  # ok to use for value? seems kind of silly
                                  index_attributes={'task_id': task_id,
                                                    'constant': '1',    # used for being able to pull all tasks
                                                    'user_id': user_id,
                                                    'start_time': starttime,
                                                    'end_time': endtime,
                                                    'interval_seconds':msg_interval,
                                                    'desired_origin': desired_origin,
                                                    'payload': payload})

        # Now that task is stored into registry, add to messaging callback
        log.debug('Adding task to scheduler')

        self._schedule_event(starttime, msg_interval, task_id)

        log.debug('Add completed OK')

        yield self.reply_ok(msg, resp)

    @defer.inlineCallbacks
    def op_rm_task(self, content, headers, msg):
        """
        Remove a task from the list/store. Will be dropped from the reactor
        when the timer fires and _send_and_reschedule checks the registry.
        """
        task_id = content.task_id

        if not task_id:
            err = 'required argument task_id not found in message'
            log.error(err)
            self.reply_err(msg, {'value': err})
            return

        # if the task is active, remove it
        if self._callback_tasks.has_key(task_id):
            self._callback_tasks[task_id].cancel()
            del self._callback_tasks[task_id]

        log.debug('Removing task_id %s from store...' % task_id)
        self.scheduled_events.remove(task_id)

        resp = yield self.mc.create_instance(RMTASK_RSP_TYPE)
        resp.value = 'OK'

        log.debug('Removal completed')
        yield self.reply_ok(msg, resp)

    ##################################################
    # Internal methods

    @defer.inlineCallbacks
    def _send_and_reschedule(self, task_id):
        """
        Check to see if we're still in the store - if not, we've been removed
        and should abort the run.
        """
        log.debug('Worker activated for task %s' % task_id)

        q = Query()
        q.add_predicate_eq('task_id', task_id)

        tdefs = yield self.scheduled_events.query(q)
        assert len(tdefs) == 1

        tdef = tdefs.values()[0]

        # pop callback object off of scheduled items
        assert self._callback_tasks.has_key(task_id)
        del self._callback_tasks[task_id]

        # deserialize and objectify payload
        # @TODO: this is costly, should keep cache?
        repo = self.workbench.create_repository()
        payload = repo._load_element(StructureElement.parse_structure_element(tdef['payload']))

        log.debug('Time to send "%s" to "%s", id "%s"' % \
                      (payload, tdef['desired_origin'], task_id))

        yield self.pub.create_and_publish_event(origin=tdef['desired_origin'],
                                                task_id=tdef['task_id'],
                                                user_id=tdef['user_id'],
                                                payload=payload)

        log.debug('Send completed, rescheduling %s' % task_id)

        # start time of None is fine, we just happened so we can be sure interval_seconds is just about right
        self._schedule_event(None, tdef['interval_seconds'], task_id)

        """
        Update last-invoked timestamp in registry
        @bug This code is commented out as it causes a run-time race condition with op_rm_task -
        splitting the read and this write fails quite often.

#        log.debug('Updating last-run time')
#        tdef['last_run'] = time.time()
#        self.store.put(task_id, tdef)
        """

        log.debug('Task %s rescheduled for %f seconds OK' % (task_id, tdef['interval_seconds']))

class SchedulerServiceClient(ServiceClient):
    """
    Client class for the SchedulerService, simple muster/send/reply.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'scheduler'
        ServiceClient.__init__(self, proc, **kwargs)
        self.mc = MessageClient(proc=proc)

    @defer.inlineCallbacks
    def add_task(self, msg):
        """
        @brief Add a recurring task to the scheduler
        @param msg protocol buffer
        @GPB(Input,2601,1)
        @GPB(Output,2602,1)
        @retval Task ID and origin
        """
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('add_task', msg)
        defer.returnValue(ret)


    @defer.inlineCallbacks
    def rm_task(self, msg):
        """
        @brief Remove a task from the scheduler
        @note If using cassandra, writes are delayed
        @param msg protocol buffer
        @GPB(Input,2603,1)
        @GPB(Output,2604,1)
        @retval OK or error
        """
        #log.info("In SchedulerServiceClient: rm_task")
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('rm_task', msg)
        defer.returnValue(ret)

# Spawn of the process using the module name
factory = ProcessFactory(SchedulerService)
