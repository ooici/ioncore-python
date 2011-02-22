#!/usr/bin/env python

"""
@file ion/services/dm/inventory/index_service.py
@author David Stuebe
@brief A service to provide indexing and search capability of objects in the datastore
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils


### Need other objects here
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)


class IndexServiceError(Exception):
    """
    An exception class for the Index Service
    """


class IndexService(ServiceProcess):
    """
    The Index Service
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='index_service',
                                             version='0.1.0',
                                             dependencies=[])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        self.mc = MessageClient(proc=self)
        
        log.info('SLC_INIT Index Service')

    @defer.inlineCallbacks
    def op_find_subject(self, predicate_object, headers, msg):
        log.info('op_hello_person: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if predicate_object.MessageType != predicate_object_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise IndexServiceError('Unexpected type received \n %s' % str(predicate_object))
            
                
        
        
        
        
        
        list_of_subjects = yield self.mc.create_instance(person_type, name='reply message')
        
        # If you want to move the whole object, you can do that using the getter/setter
        person_reply.MessageObject = person_msg.MessageObject
        
        # Change something...
        
        person_reply.name = person_reply.name + ' stuebe'
        
        # The following line shows how to reply to a message
        # The api for reply may be refactored later on so that there is just the one argument...
        yield self.reply_ok(msg, person_reply)

    


class HelloMessageClient(ServiceClient):
    """
    This is an exemplar service client that calls the hello message service. It
    makes service calls RPC style using GPB object. There is no special handling
    here, just call send. The clients should become exteremly thin wrappers with
    no business logic.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_message"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def hello_person(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('hello_person', msg)
        
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def hello_everyone(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('hello_everyone', msg)
        
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(HelloMessage)


