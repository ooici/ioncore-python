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

from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)
"""
package net.ooici.play;

// Copied from the google example!
// Changed rules - never use required!

import "net/ooici/core/link/link.proto";

message Person {
  enum _MessageTypeIdentifier {
    _ID = 20001;
    _VERSION = 1;
  }
  optional string name = 1;
  optional int32 id = 2;        // Unique ID number for this person.
  optional string email = 3;

  enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
  }

  message PhoneNumber {
    optional string number = 1;
    optional PhoneType type = 2 [default = HOME];
  }

  repeated PhoneNumber phone = 4;
}

// Our address book file is just one of these.
message AddressBook {
  enum _MessageTypeIdentifier {
    _ID = 20002;
    _VERSION = 1;
  }
  repeated Person person = 1;
  optional Person owner = 2;
  optional string title = 3;
}


// Our address book file is just one of these.
message AddressLink {
  enum _MessageTypeIdentifier {
    _ID = 20003;
    _VERSION = 1;
  }
  repeated net.ooici.core.link.CASRef person = 1;
  optional net.ooici.core.link.CASRef owner = 2;
  optional string title = 3;
}
"""


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
        self.mc = MessageClient(proc=self)
        
        log.info('SLC_INIT HelloProcess')

    @defer.inlineCallbacks
    def op_hello_person(self, person_msg, headers, msg):
        log.info('op_hello_person: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if person_msg.MessageType != person_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloMessageError('Unexpected type received \n %s' % str(person_msg))
            
                
        # Creepy hello person object log statements...
        log.info( 'Hello ' + person_msg.name +'...')
        log.info('I know your phone number ' + person_msg.name + '... it is: '+ person_msg.phone[0].number)
        
        person_reply = yield self.mc.create_instance(MessageContentTypeID=person_type)
        
        # If you want to move the whole object, you can do that using the getter/setter
        person_reply.MessageObject = person_msg.MessageObject
        
        # Change something...
        
        person_reply.name = person_reply.name + ' stuebe'
        
        # The following line shows how to reply to a message
        # The api for reply may be refactored later on so that there is just the one argument...
        yield self.reply_ok(msg, person_reply)

    @defer.inlineCallbacks
    def op_hello_everyone(self, addressbook_msg, headers, msg):
        log.info('op_hello_everyone: ')

        if addressbook_msg.MessageType  != addresslink_type:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloError('Unexpected type received \n %s' % str(addressbook_msg))
            
        log.info('Received addresbook; Title: ' + addressbook_msg.title)
        for person in addressbook_msg.person:
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


