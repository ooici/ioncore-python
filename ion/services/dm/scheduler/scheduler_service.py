#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/scheduler_service.py
@date 9/21/10
@author Paul Hubbard
@package ion.services.dm.scheduler.service Implementation of the scheduler
"""

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
    optional uint64 time_start_unix   = 4;
    optional uint64 time_end_unix     = 5;
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

    def slc_init(self):
        # @note Might want to start another AS instance with a different target name
        self.store = AttributeStoreClient(targetname='attributestore')
        self.mc = MessageClient(proc=self)

    def slc_stop(self):
        log.debug('SLC stop of Scheduler')

    def slc_deactivate(self):
        """
        Called before terminate, this is a good place to tear down the AS and jobs.
        @todo iterate over the list
        foreach task in op_query:
          rm_task(task)
        """

    def slc_shutdown(self):
        log.debug('SLC shutdown of Scheduler')

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
            task_id = str(uuid4())
            msg_interval = content.interval_seconds
        except KeyError, ke:
            log.exception('Required keys in payload not found!')
            yield self.reply_err(msg, {'value': str(ke)})
            return

        log.debug('ok, gotta task to save')


        #FIXME, generate origin if desired_origin was not provided
        if not content.IsFieldSet('desired_origin'):
            log.debug('FIXME, generate an origin because one was not provided')
            content.desired_origin = 'SOMEONE PLEASE FIXME' + str(uuid4())

        #create the response: task_id and actual origin
        resp = yield self.mc.create_instance(ADDTASK_RSP_TYPE)
        resp.task_id = task_id
        resp.origin = content.desired_origin

        # Just drop the entire message payload in
        rc = yield self.store.put(task_id, msg)
        if re.search('rror', rc):
            yield self.reply_err(msg, 'Error "%s" adding task to registry!' % rc)
            return

        # Now that task is stored into registry, add to messaging callback
        log.debug('Adding task to scheduler')
        reactor.callLater(msg_interval, self._send_and_reschedule, task_id)
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

        log.debug('Removing task_id %s from store...' % task_id)
        yield self.store.remove(task_id)

        resp = yield self.mc.create_instance(RMTASK_RSP_TYPE)
        resp.value = 'OK'

        log.debug('Removal completed')
        yield self.reply_ok(msg, resp)

    @defer.inlineCallbacks
    def op_query_tasks(self, content, headers, msg):
        """
        Query tasks registered, returns a maybe-empty list
        """
        log.debug('Looking for matching tasks')
        tlist = yield self.store.query(content.task_regex)

        log.debug(tlist)

        resp = yield self.mc.create_instance(RMTASK_RSP_TYPE)
        for t in tlist:
            resp.task_ids.add(t)
        
        self.reply_ok(msg, resp)

    ##################################################
    # Internal methods

    @defer.inlineCallbacks
    def _send_and_reschedule(self, task_id):
        """
        Check to see if we're still in the store - if not, we've been removed
        and should abort the run.
        """
        log.debug('Worker activated for task %s' % task_id)
        tdef = yield self.store.get(task_id)
        if tdef is None:
            log.info('Task ID missing in store, assuming removal and aborting')
            return

        payload    = tdef.payload
        target_id  = tdef.desired_origin
        interval   = tdef.interval_seconds

        log.debug('Time to send "%s" to "%s", id "%s"' % (payload, target_id, task_id))
        yield self.send(target_id, 'scheduler', payload)
        log.debug('Send completed, rescheduling %s' % task_id)

        reactor.callLater(interval, self._send_and_reschedule, task_id)

        """
        Update last-invoked timestamp in registry
        @bug This code is commented out as it causes a run-time race condition with op_rm_task -
        splitting the read and this write fails quite often.

#        log.debug('Updating last-run time')
#        tdef['last_run'] = time.time()
#        self.store.put(task_id, tdef)
        """
        log.debug('Task %s rescheduled for %f seconds OK' % (task_id, interval))

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

        ret = yield self.rpc_send('add_task', msg)
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


        ret = yield self.rpc_send('rm_task', msg)
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def query_tasks(self, msg):
        """
        @brief Query tasks in the scheduler by regex. Use '.+' to return all.
        @param msg protocol buffer
        @GPB(Input,2605,1)
        @GPB(Output,2606,1)
        @retval GPB containing a list, possibly zero-length.
        """
        yield self._check_init()
        ret = yield self.rpc_send('query_tasks', msg)
        defer.returnValue(ret)

# Spawn of the process using the module name
factory = ProcessFactory(SchedulerService)
