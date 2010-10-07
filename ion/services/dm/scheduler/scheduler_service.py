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
from uuid import uuid4

from ion.services.dm.scheduler.scheduler_registry import SchedulerRegistryClient
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.attributestore import AttributeStoreClient

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
            task_id = str(uuid4())
            target = content['target']
            msg_payload = content['payload']
            msg_interval = float(content['interval'])
        except KeyError, ke:
            log.exception('Required keys in payload not found!')
            yield self.reply_err(msg, {'value': str(ke)})
            return

        log.debug('ok, gotta task to save')

        # Just drop the entire message payload in
        rc = yield self.store.put(task_id, content)
        if rc != 'OK':
            yield self.reply_err(msg, 'Error adding task to registry!')
            return

        # Now that task is stored into registry, add to messaging callback
        self._schedule_next(task_id)
        self.reply_ok(msg, 'Task ID %s scheduled' % task_id)

    @defer.inlineCallbacks
    def op_rm_task(self, content, headers, msg):
        """
        Remove a task from the list/store. Will be dropped from the reactor
        when the timer fires and _send_message checks the registry.
        """
        try:
            task_id = content['task_id']
        except KeyError:
            err = 'required argument task_id not found in message'
            log.exception(err)
            self.reply_err(msg, {'value': err})
            return

        self.store.remove(task_id)
        self.reply_ok(msg, {'value': 'task removed'})

    @defer.inlineCallbacks
    def op_query_tasks(self, content, headers, msg):
        """
        Query tasks registered, returns a maybe-empty list
        """
        try:
            task_regex = content['task_regex']
        except KeyError:
            log.exception('Missing argument task_regex')
            self.reply_err(msg, {'value' : 'Missing task regex'})
            return

        log.debug('Looking for matching tasks')
        tlist = yield self.store.query(content['task_regex'])
        log.debug('%d tasks found' % len(tlist))

        self.reply_ok(msg, tlist)

    ##################################################
    # Internal methods

    @defer.inlineCallbacks
    def _send_message(self, task_id, target_id, payload):
        """
        Check to see if we're still in the store - if not, we've been removed
        and should abort the run.
        """
        tdef = yield self.store.get(task_id)
        if not tdef:
            log.info('Task ID missing on send, assuming removal and aborting')
            return

        # Do work, then reschedule ourself
        sc = ServiceClient(target=target_id)
        sc.send(payload)
        # @note fire and forget; don't need to wait for send to run to completion.

        # Schedule next invocation
        self._schedule_next(task_id)

    @defer.inlineCallbacks
    def _schedule_next(self, task_id):
        # Reschedule the next periodic invocation using the Twisted reactor

        # Pull the task def from the registry
        tdef = yield self.store.get(task_id)
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
        self.store.put(task_id, tdef)

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
