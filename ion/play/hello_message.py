#!/usr/bin/env python

"""
@file ion/play/hello_message.py
@author David Stuebe
@brief An example process definition that can be used as template for message communication.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.messaging import message_client
from ion.core.object import object_utils

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)

class HelloMessageError(Exception):
    """
    An exception class for the Hello Message example
    """


class HelloMessage(ServiceProcess):
    """
    Example process sends message objects
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_message',
                                             version='0.1.0',
                                             dependencies=[])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        self.mc = message_client.MessageClient(proc=self)
        
        log.info('SLC_INIT HelloProcess')

    @defer.inlineCallbacks
    def op_hello_person(self, person, headers, msg):
        log.info('op_hello_person: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if person.MessageType != person_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloMessageError('Unexpected type received \n %s' % str(person))
            
                
        # Creepy hello person object log statements...
        log.info( 'Hello ' + person.name +'...')
        log.info('I know your phone number ' + person.name + '... it is: '+ person.phone[0].number)
        
        # Change something...
        
        person.name = person.name + ' stuebe'
        
        # Print the name of the repository which holds this object
        log.info('The name of the person object in the service: ' +person.Repository.repository_key)
        
        # The following line shows how to reply to a message
        # The api for reply may be refactored later on so that there is just the one argument...
        yield self.reply_ok(msg, person)

    @defer.inlineCallbacks
    def op_hello_everyone(self, addressbook, headers, msg):
        log.info('op_hello_everyone: ')

        if addressbook.MessageType  != addresslink_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloError('Unexpected type received \n %s' % str(addressbook))
            
        log.info('Received addresbook; Title: ' + addressbook.title)
        for person in addressbook.person:
            log.info('Logging Person: \n' +str(person))
            
        yield self.reply_ok(msg)


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



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
