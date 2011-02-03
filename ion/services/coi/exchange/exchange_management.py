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

import ion.services.coi.exchange.exchange_boilerplate as bp
from ion.services.coi.exchange.exchange_boilerplate import ServiceHelper

CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)


class ExchangeManagementService(ServiceProcess):


    
    # Declaration of service
    declare = ServiceProcess.service_declare(name='exchange_management',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        ServiceProcess.__init__(self, *args, **kwargs)
        
    def slc_init(self):
        self.helper = ServiceHelper(self)
        self.xs = {}
        self.xn = {}
        log.debug("ExchangeManagementService.slc_init(self)")
      


    @defer.inlineCallbacks
    def op_create_object(self, object, headers, msg):
        """
        For testing purposes only.  
        """
        log.debug('op_create_object()')
        object = yield self.helper.create_object(
                    object, 
                    "TestObject", 
                    "This is not a valid system object."
        )
        response = yield self.helper.push_object(object)
        yield self.reply_ok(msg, response.resource_reference)


    @defer.inlineCallbacks
    def op_get_object(self, sha1, headers, msg):
        """
        For testing purposes only.  
        """
        log.debug('op_get_object()')
        object = yield self.helper.get_object(sha1)
        yield self.reply_ok(msg, object)


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
        
        # Field validation
        try:
            name = exchangespace.configuration.name.strip()
            description = exchangespace.configuration.description
            if len(name) == 0:
                raise bp.ExchangeManagementError("exchangespace.name is invalid") 
            if self.xs.has_key(name):
                raise bp.ExchangeManagementError("exchangespace.name already exists") 
            
        except bp.ExchangeManagementError, err:
            yield self.reply_err(msg, str(err))
            return
        
        # Field population
        object.name = name
        object.description = description
        
        
        # Response
        response = yield self.helper.push_object(object)
        self.xs[name] = response.configuration.MyId;
        log.debug('Created exchangespace.  id: %s', response.configuration.MyId)
        yield self.reply_ok(msg, response.configuration.MyId)



    @defer.inlineCallbacks
    def op_create_exchangename(self, exchangename, headers, msg):
        """
        Creates an ExchangeSpace distributed resource from the parameter 
        exchangespace.  The following restrictions are enforced:  request.name 
        must be defined, must be a uniquely named ExchangeSpace, and must 
        not already exist in the system.  request.description must not be
        a trivial string and should provide a useful description of the
        ExchangeSpace.        
        """
        log.debug('op_create_exchangename()')
        
        # Object creation
        object = yield self.helper.create_object(exchangename, "Name", "Description")
        
        # Field validation
        try:
            name = exchangename.configuration.name.strip()
            description = exchangename.configuration.description
            exchangespace = exchangename.configuration.exchangespace.strip()
            if len(name) == 0:
                raise bp.ExchangeManagementError("exchangename.name is required") 
            if self.xn.has_key(name):
                raise bp.ExchangeManagementError("exchangename.name already exists") 
            if len(exchangespace) == 0:
                raise bp.ExchangeManagementError("exchangename.exchangespace is required") 
            if not self.xs.has_key(exchangespace):
                raise bp.ExchangeManagementError("exchangename.exchangespace doesn't exist") 
            
        except bp.ExchangeManagementError, err:
            yield self.reply_err(msg, str(err))
            return
        
        # Field population
        object.name = name
        object.description = description
        
        
        # Response
        response = yield self.helper.push_object(object)
        self.xn[name] = response.configuration.MyId;
        log.debug('Created exchangename.  id: %s', response.configuration.MyId)
        yield self.reply_ok(msg, response.configuration.MyId)


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


    @defer.inlineCallbacks
    def _get_object(self, msg):
        """
        Used for testing purposes only.
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_object', msg)
        defer.returnValue(content)




    @defer.inlineCallbacks
    def create_exchangespace(self, msg):
        """
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('create_exchangespace', msg)
        defer.returnValue(content)

        
    @defer.inlineCallbacks
    def create_exchangename(self, msg):
        """
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('create_exchangename', msg)
        defer.returnValue(content)

        

factory = ProcessFactory(ExchangeManagementService)



