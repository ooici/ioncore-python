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

from ion.core.base_process import ProcessFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.dm.scheduler.scheduler_registry import SchedulerRegistry

class SchedulerService(BaseService):
    """

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

    def op_rm_task(self, content, headers, msg):
        """
        Remove a task from the list
        """
        yield self.reply_err(msg, {'value':'Not implemented!'}, {})

    def op_query_tasks(self, content, headers, msg):
        """
        Query tasks registered, returns a maybe-empty list
        """
        yield self.reply_err(msg, {'value':'Not implemented!'}, {})

    def op_update_task(self, content, headers, msg):
        """
        Redefine an existing task
        @todo Necessary feature?
        """
        yield self.reply_err(msg, {'value':'Not implemented!'}, {})


class SchedulerServiceClient(BaseServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'scheduler'
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def add_task(self, target, payload):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('add_task', target, payload)
        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProcessFactory(SchedulerService)
