#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/scheduler_registry.py
@date 9/21/10
@author Paul Hubbard
@package ion.services.dm.scheduler.registry Persistent registry of scheduled messages (crontab)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.data.store import IStore

class SchedulerRegistry(BaseService):
    # Declaration of service
    declare = BaseService.service_declare(name='scheduler_registry',
                                          version='0.1.0',
                                          dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        self.store = IStore()
        yield self.store.create_store()

        # @bug Clear store at init - remove once debugged!
        log.warn('Clearing registry!')
        yield self.store.clear_store()

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


        self.reply_err(msg, {'value':'Not implemented!'}, {})

    def op_rm_task(self, content, headers, msg):
        self.reply_err(msg, {'value':'Not implemented!'}, {})

    def op_query_tasks(self, content, headers, msg):
        self.reply_err(msg, {'value':'Not implemented!'}, {})

class SchedulerRegistryClient(BaseServiceClient):

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'scheduler_service'
        BaseServiceClient.__init__(self, proc, **kwargs)

    def add_task(self, taskid, interval, payload):
        pass

    def rm_task(self, taskid):
        pass

    def query_tasks(self, task_regex):
        pass

# Spawn of the process using the module name
factory = ProtocolFactory(SchedulerRegistry)
