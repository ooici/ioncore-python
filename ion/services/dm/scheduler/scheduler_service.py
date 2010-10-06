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
import time

from ion.services.dm.scheduler.scheduler_registry import SchedulerRegistryClient
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

class SchedulerService(ServiceProcess):
    """
    First pass at a message-based cron service, where you register a send-to address,
    interval and payload, and the scheduler will message you when the timer expires.
    @note this will be subsumed into CEI at some point; consider this a prototype.
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='scheduler',
                                          version='0.1.0',
                                          dependencies=['scheduler_registry'])

    def slc_init(self):
        self.ctab = SchedulerRegistryClient()

    @defer.inlineCallbacks
    def op_add_task(self, content, headers, msg):
        """
        @brief Add a new task to the crontab. Interval is in seconds, fractional.
        @param content Message payload, must be a dictionary with 'target', 'interval' and 'payload' keys
        @param headers Ignored here
        @param msg Ignored here
        @retval reply_ok or reply_err
        """
        try:
            tid = content['target']
            msg_payload = content['payload']
            msg_interval = float(content['interval'])
        except KeyError, ke:
            log.exception('Required keys in payload not found!')
            yield self.reply_err(msg, {'value': str(ke)})
            return

        log.debug('ok, gotta task to save')
        task_id = yield self.ctab.store_task(tid, msg_interval, payload=msg_payload)
        if task_id:
            yield self.reply_ok(msg, task_id)
        else:
            yield self.reply_err(msg, 'Error adding task to registry!')

        # Now that task is stored into registry, add to messaging callback
        self._schedule_next(task_id)
        self.reply_ok(msg, 'Task ID %s scheduled' % task_id)

    def _send_message(self, task_id, target_id, payload):
        # Do work, then reschedule ourself
        #bpc = BaseProcessClient(target=target_id)
        # @note fire and forget; don't need to wait for send to run to completion.
        #bpc.send(payload)

        # Schedule next invocation
        self._schedule_next(task_id)

    @defer.inlineCallbacks
    def _schedule_next(self, task_id):
        # Pull the task def from the registry
        tdef = yield self.ctab.query_tasks(task_id)

        try:
            target_id = tdef['target']
            interval = tdef['interval']
            payload = tdef['payload']
        except KeyError:
            log.exception('Error parsing task def from registry! Task id: "%s"' % task_id)
            defer.returnValue(None)

        reactor.callLater(interval, self._send_message, task_id, target_id, payload)

        # Update last-invoked timestamp in registry
        tdef['last_run'] = time.time()
        yield self.ctab.store_task(tdef)

    @defer.inlineCallbacks
    def op_rm_task(self, content, headers, msg):
        """
        Remove a task from the list
        """
        yield self.reply_err(msg, {'value':'Not implemented!'}, {})

    @defer.inlineCallbacks
    def op_query_tasks(self, content, headers, msg):
        """
        Query tasks registered, returns a maybe-empty list
        """
        yield self.reply_err(msg, {'value':'Not implemented!'}, {})

class SchedulerServiceClient(ServiceClient):
    """
    Client class for the SchedulerService, simple muster/send/reply.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'scheduler'
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def add_task(self, target, interval, payload):
        yield self._check_init()
        msg_dict = {'target': target, 'payload': payload, 'interval': interval}
        (content, headers, msg) = yield self.rpc_send('add_task', msg_dict)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def rm_task(self, taskid):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('rm_task', taskid)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def query_tasks(self, task_regex):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('query_tasks', task_regex)
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(SchedulerService)
