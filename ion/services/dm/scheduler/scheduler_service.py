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
import threading
import re
from uuid import uuid4

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

    def slc_stop(self):
        log.debug('SLC stop of Scheduler')

    def slc_shutdown(self):
        log.debug('SLC shutdown of Scheduler')

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
            msg_interval = float(content['interval'])
        except KeyError, ke:
            log.exception('Required keys in payload not found!')
            yield self.reply_err(msg, {'value': str(ke)})
            return

        log.debug('ok, gotta task to save')

        # Just drop the entire message payload in
        rc = yield self.store.put(task_id, content)
        if re.search('rror', rc):
            yield self.reply_err(msg, 'Error "%s" adding task to registry!' % rc)
            return

        # Now that task is stored into registry, add to messaging callback
        log.debug('Adding task to scheduler')
        reactor.callLater(msg_interval, self._send_and_reschedule, task_id)
        log.debug('Add completed OK')
        yield self.reply_ok(msg, {'value':task_id})

    @defer.inlineCallbacks
    def op_rm_task(self, content, headers, msg):
        """
        Remove a task from the list/store. Will be dropped from the reactor
        when the timer fires and _send_and_reschedule checks the registry.
        """
        task_id = content

        if not task_id:
            err = 'required argument task_id not found in message'
            log.error(err)
            self.reply_err(msg, {'value': err})
            return

        log.debug('Removing task_id %s from store...' % task_id)
        yield self.store.remove(task_id)
        log.debug('Removal completed')
        yield self.reply_ok(msg, {'value': 'OK'})

    @defer.inlineCallbacks
    def op_query_tasks(self, content, headers, msg):
        """
        Query tasks registered, returns a maybe-empty list
        """
        log.debug('Looking for matching tasks')
        tlist = yield self.store.query(content)

        log.debug(tlist)

        self.reply_ok(msg, tlist)

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
        if tdef == None:
            log.info('Task ID missing in store, assuming removal and aborting')
            return

        payload = tdef['payload']
        target_id = tdef['target']
        interval = tdef['interval']

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

    @defer.inlineCallbacks
    def add_task(self, target, interval, payload):
        """
        @brief Add a recurring task to the scheduler
        @param target Destination address, available via self._get_procid
        @param interval Time, in fractional seconds, between messages
        @param payload Optional payload to include in the scheduled messages
        @retval Task ID, a GUID used as a key for rm_task
        """
        yield self._check_init()
        msg_dict = {'target': target, 'payload': payload, 'interval': interval}
        (content, headers, msg) = yield self.rpc_send('add_task', msg_dict)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def rm_task(self, taskid):
        """
        @brief Remove a task from the scheduler
        @note If using cassandra, writes are delayed
        @param taskid Task ID, as returned from add_task
        @retval OK or error
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('rm_task', taskid)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def query_tasks(self, task_regex):
        """
        @brief Query tasks in the scheduler by regex. Use '.+' to return all.
        @retval List, possibly zero-length.
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('query_tasks', task_regex)
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(SchedulerService)
