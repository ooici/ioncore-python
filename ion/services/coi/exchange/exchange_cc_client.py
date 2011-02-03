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

from ion.services.coi.exchange.exchange_boilerplate import ServiceHelper

CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)


class ExchangeManagementService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='exchange_management',
                                             version='0.1.0',
                                             dependencies=[])

    def slc_init(self):
        self.helper = ServiceHelper(self)
        log.debug("ExchangeManagementService.slc_init(self)")
      


    @defer.inlineCallbacks
    def op_create_object(self, object, headers, msg):
        """
        For testing purposes only.  
        """
        log.debug('op_create_exchangespace()')
        object = yield self.helper.create_object(
                    object, 
                    "TestObject", 
                    "This is not a valid system object."
        )
        response = yield self.helper.push_object(object)
        log.debug('Created exchangespace.  id: %s', response.configuration.MyId)
        yield self.reply_ok(msg, response.configuration.MyId)


    # EXCHANGESPACE CRUD

    @defer.inlineCallbacks
    def op_create_exchangespace(self, exchangespace, headers, msg):
        """
        Creates an ExchangeSpace distributed resource from the parameter 
        exchangespace.  The following restrictions are enforced:  request.name 
        must be defined, must be a uniquely named ExchangeSpace, and must 
        not already exist in the system.  request.description must not be
        a trivial string and should provide a useful description of the
        ExchangeSpace.        
        """
        log.debug('op_create_exchangespace()')
        
        # Object creation
        object = yield self.helper.create_object(exchangespace, "Name", "Description")
        
        # Field validation and population
        object.name = exchangespace.configuration.name
        object.description = exchangespace.configuration.description
        
        # Response
        response = yield self.helper.push_object(object)
        log.debug('Created exchangespace.  id: %s', response.configuration.MyId)
        yield self.reply_ok(msg, response.configuration.MyId)


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
        log.debug("ExchangeManagementClient.__init__(self, proc, args)")
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "exchange_management"
        ServiceClient.__init__(self, proc, **kwargs)


    @defer.inlineCallbacks
    def _create_object(self, msg):
        """
        Used for testing purposes only.
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('create_object', msg)
        defer.returnValue(content)


    # EXCHANGE SPACE CLIENT METHODS

    @defer.inlineCallbacks
    def create_exchangespace(self, msg):
        """
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('create_exchangespace', msg)
        defer.returnValue(content)

        
    @defer.inlineCallbacks
    def update_exchangespace(self, msg):
        """
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('update_exchangespace', msg)


    @defer.inlineCallbacks
    def set_exchangespace_life_cycle(self, msg):
        """
        """
        yield self._check_init()
        yield self.send('set_exchangespace_life_cycle', msg)



    # EXCHANGE NAME CLIENT METHODS

    @defer.inlineCallbacks
    def create_exchangename(self, msg):
        """
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('create_exchangename', msg)

        
    @defer.inlineCallbacks
    def update_exchangename(self, msg):
        """
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('update_exchangename', msg)


    @defer.inlineCallbacks
    def set_exchangename_life_cycle(self, msg):
        """
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('set_exchangename_life_cycle', None)


factory = ProcessFactory(ExchangeManagementService)




