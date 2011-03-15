#!/usr/bin/env python

"""
@file ion/play/hello_resource.py
@author David Stuebe
@brief An example service definition that can be used as template for resource management.
"""

import ion.util.ionlog
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core import ioninit
from twisted.internet import defer

import ion.services.coi.exchange.resource_wrapper as res_wrapper
from ion.services.coi.exchange.resource_wrapper import ServiceHelper, ClientHelper
from ion.services.coi.exchange.broker_controller import BrokerController
from ion.services.coi.exchange.exchange_types import ExchangeTypes

CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)


class EMSError(Exception):
    """
    An error class for the ems...
    """

class ExchangeManagementService(ServiceProcess):


    
    # Declaration of service
    declare = ServiceProcess.service_declare(name='exchange_management',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        ServiceProcess.__init__(self, *args, **kwargs)

    @defer.inlineCallbacks
    def slc_init(self):
        log.info("ExchangeManagementService.slc_init(self)")
        self.helper = ServiceHelper(self)
        self.controller = BrokerController()
        self.exchange_types = ExchangeTypes(self.controller)
        yield self.controller.start()
        
        self.xs = {}
        self.xn = {}
        


    @defer.inlineCallbacks
    def slc_deactivate(self):
        log.info("ExchangeManagementService.slc_terminate(self)")
        yield self.controller.stop()
        

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
                raise res_wrapper.ExchangeManagementError("exchangespace.name is invalid") 
            if self.xs.has_key(name):
                raise res_wrapper.ExchangeManagementError("exchangespace.name already exists") 
            
        except res_wrapper.ExchangeManagementError, err:
            yield self.reply_err(msg, str(err))
            return
        
        # Field population
        object.name = name
        object.description = description

        # Response
        response = yield self.helper.push_object(object)
        self.xs[name] = response.configuration.MyId;
        log.debug('Created exchangespace.  id: %s', response.resource_reference)
        yield self.reply_ok(msg, response)



    @defer.inlineCallbacks
    def op_create_exchangename(self, exchangename, headers, msg):
        """
        Creates an ExchangeSpace distributed resource from the parameter 
        exchangespace.  The following restrictions are enforced:  request.name 
        must be defined, must be a uniquely named ExchangeSpace, and must 
        not already exist in the system.  request.description must not be
        a trivial string and should provide a useful description of the
        ExchangeSpace.        
        
        net.ooici.services.coi.exchange_management.proto defines
        the following Exchange types:
            PROCESS = 1;
            SERVICE = 2;
            EXCHANGE_POINT = 3;
            QUEUE = 4;

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
                raise res_wrapper.ExchangeManagementError("exchangename.name is required") 
            if self.xn.has_key(name):
                raise res_wrapper.ExchangeManagementError("exchangename.name already exists") 
            if len(exchangespace) == 0:
                raise res_wrapper.ExchangeManagementError("exchangename.exchangespace is required") 
            if not self.xs.has_key(exchangespace):
                raise res_wrapper.ExchangeManagementError("exchangename.exchangespace doesn't exist") 
            
        except res_wrapper.ExchangeManagementError, err:
            yield self.reply_err(msg, str(err))
            return
        
        # Field population
        object.name = name
        object.description = description
        
        #if object.type
        yield self.exchange_types.create_exchange_point(exchangespace, name)

        
        # Response
        response = yield self.helper.push_object(object)
        self.xn[name] = response.configuration.MyId;
        log.debug('Created exchangename.  id: %s', response.configuration.MyId)
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_create_queue(self, queue, headers, msg):
        """
        Creates a queue and binds it to the appropriate namespace and exchange.
        """
        q = queue.MessageObject
        desc = q.configuration.description
        qname = q.configuration.name
        xn = q.configuration.exchangename
        xs = q.configuration.exchangespace
        
        self.controller.create_queue(xs + "." + xn)
        
        log.debug('op_create_queue()')
        
        # Object creation
        yield self.reply_ok(msg, None)


    @defer.inlineCallbacks
    def op_create_binding(self, binding, headers, msg):
        """
        Creates a queue and binds it to the appropriate namespace and exchange.
        """
        b = binding.MessageObject
        desc = b.configuration.description
        bname = b.configuration.name
        xn = b.configuration.exchangename
        xs = b.configuration.exchangespace
        topic = b.configuration.topic
        q = b.configuration.queuename
        
        self.controller.bind(
                name = xs + "." + xn + "." + q,
                exchange = xs + "." + xn,
                routing_key = topic
        )
        
        log.debug('op_create_queue()')
        
        # Object creation
        yield self.reply_ok(msg, None)


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
        log.info("ExchangeManagementService.slc_init(...)")
        self.helper = ClientHelper(proc)
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
    def create_exchangespace(self, 
            name,
            description,
            ):
        """
        Creates an ExchangeSpace.
        @param name
                a string uniquely identifying the ExchangeSpace 
                in all scopes and contexts.
        @param description 
                a free text string containing a description of 
                the ExchangeSpace.
        """
        yield self._check_init()
        msg = yield self.helper.create_object(res_wrapper.exchangespace_type)
        msg.configuration.name = name
        msg.configuration.description = description
        
        (content, headers, msg) = yield self.rpc_send('create_exchangespace', msg)
        defer.returnValue(content)

        
    @defer.inlineCallbacks
    def create_exchangename(
            self,
            name,
            description,
            exchangespace,
            type='EXCHANGE_POINT', 
        ):
            """
            Creates an ExchangeName.
            @param name 
                    a string uniquely identifying the ExchangeName 
                    in the scope of the ExchangeSpace.
            @param description 
                    a free text string containing a description of 
                    the ExchangeName.
            @param exchangespace
                    a string uniquely identifying the ExchangeSpace
                    to which this ExchangeName will belong.  This 
                    must be previously defined with a call to 
                    create_exchangespace()
            @param type
                    a string that must contain one of the following
                    constants:  'EXCHANGE_POINT', 'PROCESS', 'SERVICE'.
            """        
            yield self._check_init()
    
            msg = yield self.helper.create_object(res_wrapper.exchangename_type)
            msg.configuration.name = name
            msg.configuration.description = description
            msg.configuration.exchangespace = exchangespace
            if type == 'EXCHANGE_POINT':
                msg.configuration.type = msg.configuration.Type.EXCHANGE_POINT
            elif type == 'PROCESS':
                msg.configuration.type = msg.configuration.Type.PROCESS
            elif type == 'SERVICE':
                msg.configuration.type = msg.configuration.Type.SERVICE
            else:
                raise EMSError('Invalid type specified in create_exchangename operation')
    
            (content, headers, msg) = yield self.rpc_send('create_exchangename', msg)
            defer.returnValue(content)



    @defer.inlineCallbacks
    def create_queue(
            self,
            name,
            description,
            exchangespace,
            exchangename
            ):
            """
            Creates a Queue.
            @param name 
                    a string uniquely identifying the Queue 
                    in the scope of the ExchangeSpace and 
                    ExchangeName.
            @param description 
                    a free text string containing a description of 
                    the ExchangeName.
            @param exchangespace
                    a string uniquely identifying the ExchangeSpace
                    to which ExchangeName belongs.  This must be 
                    previously defined with a call to create_exchangespace()
            @param exchangename
                    a string uniquely identifying the ExchangeName
                    to which this queue will be bound.  This must be 
                    previously defined with a call to create_exchangename()
            """        
            yield self._check_init()
    
            msg = yield self.helper.create_object(res_wrapper.queue_type)
            msg.configuration.name = name
            msg.configuration.description = description
            msg.configuration.exchangespace = exchangespace
            msg.configuration.exchangename = exchangename
    
            (content, headers, msg) = yield self.rpc_send('create_queue', msg)
            defer.returnValue(content)


    @defer.inlineCallbacks
    def create_binding(
            self,
            name,
            description,
            exchangespace,
            exchangename,
            queuename,
            topic
        ):
            """
            Creates a Binding.
            @param name 
                    a string uniquely identifying the Queue 
                    in the scope of the ExchangeSpace and 
                    ExchangeName.
            @param description 
                    a free text string containing a description of 
                    the ExchangeName.
            @param exchangespace
                    a string uniquely identifying the ExchangeSpace
                    to which ExchangeName belongs.  This must be 
                    previously defined with a call to create_exchangespace()
            @param exchangename
                    a string uniquely identifying the ExchangeName
                    to which this queue will be bound.  This must be 
                    previously defined with a call to create_exchangename()
            """        
            yield self._check_init()
    
            msg = yield self.helper.create_object(res_wrapper.binding_type)
            msg.configuration.name = name
            msg.configuration.description = description
            msg.configuration.exchangespace = exchangespace
            msg.configuration.exchangename = exchangename
            msg.configuration.queuename = queuename
            msg.configuration.topic = topic
    
            (content, headers, msg) = yield self.rpc_send('create_queue', msg)
            defer.returnValue(content)

           

factory = ProcessFactory(ExchangeManagementService)



