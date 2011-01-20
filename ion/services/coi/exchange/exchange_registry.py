#!/usr/bin/env python


"""
@file ion/services/coi/exchange/exchange_registry.py
@author Brian Fox
@brief provides a registry service for exchange spaces and exchange points
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
from twisted.internet import defer


CONF = ioninit.config(__name__)

class ExchangeRegistryService(ServiceProcess):
    """
    @brief Skeleton exchange registry
    @todo flesh out
    """
    def __init__(self, *args, **kwargs):
        ServiceProcess.__init__(self, *args, **kwargs)
        self.spawn_args['bootstrap_args'] = self.spawn_args.get('bootstrap_args', CONF.getValue('bootstrap_args', default=None))
        self.rc = ResourceClient(proc=self)
    
    log.info('ExchangeRegistryService.__init__()')
        
    
    @defer.inlineCallbacks
    def op_get_exchangeobject(self, content, headers, msg):
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


    declare = ServiceProcess.service_declare(name='exchange_registry', version='0.1.0', dependencies=[])




class ExchangeRegistryClient(ServiceClient):
    """
    @brief Client class for accessing the ExchangeRegistry.
    @todo Stubbed only.  Needs much more work.
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "exchange_registry"
        ServiceClient.__init__(self, proc, **kwargs)


    @defer.inlineCallbacks
    def get_exchange_object(self, name):
        """
        @brief Remove a task from the scheduler
        @note If using cassandra, writes are delayed
        @param taskid Task ID, as returned from add_task
        @retval OK or error
        """
        #log.info("In SchedulerServiceClient: rm_task")
        # yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_exchangeobject', name)
        defer.returnValue(content)

factory = ProcessFactory(ExchangeRegistryService)
