#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/scheduler_service.py
@date 9/21/10
@author Paul Hubbard
@package ion.services.dm.scheduler.service Implementation of the scheduler
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.dm.scheduler.scheduler_registry import SchedulerRegistry

class SchedulerService(BaseService):
    """
    First pass at a message-based cron service, where you register a send-to address,
    interval and payload, and the scheduler will message you when the timer expires.
    @note this will be subsumed into CEI at some point; consider this a prototype.
    """
    # Declaration of service
    declare = BaseService.service_declare(name='scheduler',
                                          version='0.1.0',
                                          dependencies=[])

    def slc_init(self):
        pass

    @defer.inlineCallbacks
    def op_add_task(self, content, headers, msg):
        """
        Add a new task to the crontab
        """
        yield self.reply_err(msg, {'value':'Not implemented!'}, {})

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

class SchedulerServiceClient(BaseServiceClient):
    """
    Client class for the SchedulerService, simple muster/send/reply.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'scheduler'
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def add_task(self, target, payload):
        yield self._check_init()
        msg_dict = {'target': target, 'payload': payload}
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
factory = ProtocolFactory(SchedulerService)
