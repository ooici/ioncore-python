#!/usr/bin/env python

"""
@file ion/play/hello_resource.py
@author David Stuebe
@brief An example service definition that can be used as template for resource management.
"""

import ion.util.ionlog
import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils
from ion.core import ioninit
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.services.coi.resource_registry_beta.resource_client import ResourceClientError, ResourceInstanceError
from twisted.internet import defer


CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)


class ExchangeManagementError(Exception):
    """
    An exception class for the Exhcnage Management system.
    """


class ExchangeManagementService(ServiceProcess):

    declare = ServiceProcess.service_declare(name='exchange_management',
                                          version='0.1.0',
                                          dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        ServiceProcess.__init__(self, *args, **kwargs)
        
        self.push = self.workbench.push
        self.pull = self.workbench.pull
        self.fetch_linked_objects = self.workbench.fetch_linked_objects
        self.op_fetch_linked_objects = self.workbench.op_fetch_linked_objects
        
        self.datastore_service = self.spawn_args.get(
                'datastore_service', 
                CONF.getValue('datastore_service', default='No Data Store service name provided!')
        )
        
        log.info('ExchangeManagementService.__init__()')
        
        
    def slc_init(self):
        self.rc = ResourceClient(proc=self)
        self.mc = MessageClient(proc=self)
        self.instance_counter = 1
        log.info("ExchangeManagementService slc init.")
      

    # EXCHANGESPACE CRUD

    @defer.inlineCallbacks
    def op_create_exchangespace(self, request, headers, msg):
        """
        Creates an ExchangeSpace distributed resource from the parameter 
        request.  The following restrictions are enforced:  request.name 
        must be defined, must be a uniquely named ExchangeSpace, and must 
        not already exist in the system.  request.description must not be
        a trivial string and should provide a useful description of the
        ExchangeSpace.        
        """
        log.info('op_create_exchangespace: ')
        yield self.reply_ok(msg, "Reply")


    @defer.inlineCallbacks
    def op_update_exchangespace(self, request, headers, msg):
        """
        Updates an ExchangeSpace distributed resource using the parameter 
        request.  The following restrictions are enforced:  request.name 
        must be defined, must be a uniquely named ExchangeSpace, and must 
        not already exist in the system.  request.description must not be
        a trivial string and should provide a useful description of the
        ExchangeSpace.        
        """
        log.info('op_update_exchangespace: ')
        yield self.reply_ok(msg)


    @defer.inlineCallbacks
    def op_set_exchangespace_life_cycle(self, request, headers, msg):
        """
        Sets the ExchangeSpace resource life cycle.  This method should be
        used with care.  The ExchangeSpace object is the head of a tree
        and all nodes of that tree are updated with the provided life cycle
        state.  All changes are subject to ownership and permission check.
        """
        log.info('op_set_exchangespace_life_cycle: ')
        yield self.reply_ok(msg)
        

    # EXCHANGENAME CRUD

    @defer.inlineCallbacks
    def op_create_exchangename(self, request, headers, msg):
        """
        Creates an ExchangeName distributed resource from the parameter 
        request.   
        """
        log.info('op_create_exchangename: ')
        yield self.reply_ok(msg)


    @defer.inlineCallbacks
    def op_update_exchangename(self, request, headers, msg):
        """
        Updates an ExchangeSpace distributed resource using the parameter 
        request. 
        """
        log.info('op_update_exchangename: ')
        yield self.reply_ok(msg)


    @defer.inlineCallbacks
    def op_set_exchangename_life_cycle(self, request, headers, msg):
        """
        Sets the ExchangeName resource life cycle.  All changes are subject 
        to ownership and permission check.
        """
        log.info('op_set_exchangename_life_cycle: ')
        yield self.reply_ok(msg)
        
        
        
class ExchangeManagementClient(ServiceClient):
    
    def __init__(self, proc=None, **kwargs):
        log.info("ExchangeManagementClient init.")
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "exchange_registry"
        ServiceClient.__init__(self, proc, **kwargs)



    # EXCHANGE SPACE CLIENT METHODS

    @defer.inlineCallbacks
    def create_exchangespace(self, name, description):
        """
        """
        yield self._check_init()
        yield self.send('create_exchangespace', None)

        
    @defer.inlineCallbacks
    def update_exchangespace(self, name, description):
        """
        """
        yield self._check_init()
        yield self.send('update_exchangespace', None)


    @defer.inlineCallbacks
    def set_exchangespace_life_cycle(self, msg):
        """
        """
        yield self._check_init()
        yield self.send('set_exchangespace_life_cycle', None)



    # EXCHANGE NAME CLIENT METHODS

    @defer.inlineCallbacks
    def create_exchangename(self, msg):
        """
        """
        yield self._check_init()
        yield self.send('create_exchangename', None)

        
    @defer.inlineCallbacks
    def update_exchangename(self, msg):
        """
        """
        yield self._check_init()
        yield self.send('update_exchangename', None)


    @defer.inlineCallbacks
    def set_exchangename_life_cycle(self, msg):
        """
        """
        yield self._check_init()
        yield self.send('set_exchangename_life_cycle', None)


factory = ProcessFactory(ExchangeManagementService)




