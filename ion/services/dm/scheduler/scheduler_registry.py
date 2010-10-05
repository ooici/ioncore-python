#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/scheduler_registry.py
@date 9/21/10
@author Paul Hubbard
@package ion.services.dm.scheduler.registry Persistent registry of scheduled messages (crontab)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import uuid

from twisted.internet import defer

from ion.core.process.process import ProcessFactory

from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.data.datastore.registry import BaseRegistryService, BaseRegistryClient
from ion.data import dataobject

class SchedulerRegistry(BaseRegistryService):
    """
    Our registry is an instance of the BaseRegistryService.
    """
    declare = ServiceProcess.service_declare(name='scheduler_registry',
                                          version='0.1.0',
                                          dependencies=[])

    """
    OK, base class lacks the required op_ prefix, so you're forced
    to rename the functions to match the LCA signature. This needs a redesign.
    """
    op_clear = BaseRegistryService.base_clear_registry

    #op_store_task = BaseRegistryService.base_register_resource
    def op_store_task(self, content, headers, msg):
        log.info('got a message!')
        log.info(content)
        return self.base_register_resource(content, headers, msg)

    """
    And the base class has no remove function. Weak.

    op_rm_task = BaseRegistryService.
    """
    op_query_tasks = BaseRegistryService.base_find_resource

    def op_rm_task(self, content, headers, msg):
        self.reply_err(msg, 'Method not implemented!!')

class ScheduleEntry(dataobject.Resource):
    """
    Class representing a single stored schedule entry. Contents are
    entry is a dictionary with
    'target', 'interval', 'payload' and 'last_run' keys
    """
    id = dataobject.TypedAttribute(str, 'Not set')
    entry = dataobject.TypedAttribute(dict, {})


class SchedulerRegistryClient(BaseRegistryClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'scheduler_registry'
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def clear(self):
        """
        Nuke the registry contents.
        @todo Wrap this with auth*
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('clear', None)
        if content['status'] == 'OK':
            defer.returnValue(None)
        else:
            log.error('Error clearing registry!')

    def store_task(self, target, interval, payload=None, taskid=None):
        """
        @brief Stores a task in the registry, optionally overwriting a previous version.
        @param target Name to send to in the exchange
        @param interval periodic interval to send the message, in fractional seconds
        @param payload Payload to send in the message
        @taskid Optional - if set, will overwrite the previous task definition
        @retval taskid Unique task ID
        """
        msg = {'target': target, 'interval': interval}
        if payload:
            msg['payload'] = payload

        if taskid:
            msg['taskid'] = taskid
        else:
            msg['taskid'] = str(uuid.uuid4())

        """
        Now this blows. You have to create a Resource explicitly via a call to
        Yet Another Parent Class. I do not like this design.
        """
        se_object = ScheduleEntry()
        se_object.entry = msg
        se_object.id = msg['taskid']

        return self.base_register_resource('store_task', se_object)

    def query_tasks(self, task_regex):
        """
        @brief Query the task registry for a list of tasks that match the regex.
        @note Primary key is taskid
        @param task_regex Regular expression to match
        @retval Array of task, possibly empty. Entries in the array are a dictionary holding
         {'target', 'interval', 'payload', 'last_run'} fields.
        """
        return self.base_find_resource('query_tasks', task_regex, regex=True)

    def rm_task(self, taskid):
        """
        Remove a given taskid from the registry.
        @param taskid Task ID to remove.
        """
        se_object = ScheduleEntry()
        se_object.id = taskid
        # As per david s, this is the current way to rm something
        return self.base_set_resource_lcstate('rm_task', se_object, 'retired')

# Spawn of the process using the module name
factory = ProcessFactory(SchedulerRegistry)

# WTF?
dataobject.DataObject._types['ScheduleEntry'] = ScheduleEntry
