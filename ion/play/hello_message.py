#!/usr/bin/env python

"""
@file ion/play/hello_message.py
@author David Stuebe
@brief An example process definition that can be used as template for message communication.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils
from ion.core.exception import ApplicationError

# from net.ooici.play addressbook.proto
PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)
"""
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

"""

# from net.ooici.play addressbook.proto
ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
"""
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


class HelloMessageError(ApplicationError):
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
        """
        @brief Respond to a simple message
        @param params person_msg GPB, 20001/1, a person object from net.ooici.play.
        @retval response, GPB 20001/1, a person message if successful.
        """
        log.info('op_hello_person: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if person_msg.MessageType != PERSON_TYPE:
            # This will send an error message and reset the state of the hello service
            raise HelloMessageError('Unexpected type received \n %s' % str(person_msg), person_msg.ResponseCodes.BAD_REQUEST)
            
                
        # Creepy hello person object log statements...
        log.info( 'Hello ' + person_msg.name +'...')
        log.info('I know your phone number ' + person_msg.name + '... it is: '+ person_msg.phone[0].number)

        # Create a response message object
        person_reply = yield self.mc.create_instance(MessageContentTypeID=PERSON_TYPE)
        
        # If you want to move the whole object, you can do that using the getter/setter
        person_reply.MessageObject = person_msg.MessageObject
        
        # Change something about the reply
        person_reply.name = person_reply.name + ' stuebe'

        # Set a response code in the message - see net.ooici.core.message.ion_message.proto for details on responses
        person_reply.MessageResponseCode = person_reply.ResponseCodes.OK

        # The following line shows how to reply with a message object
        yield self.reply_ok(msg, person_reply)

    @defer.inlineCallbacks
    def op_hello_everyone(self, addresslink_msg, headers, msg):
        """
        @brief Respond to a simple message
        @param params addresslink_msg GPB, 20003/1, a addresslink object from net.ooici.play.
        @retval ack - a message envelope with ResponseCode OK and no content.
        """
        log.info('op_hello_everyone: ')

        if addresslink_msg.MessageType  != ADDRESSLINK_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise HelloMessageError('Unexpected type received \n %s' % str(addresslink_msg), addresslink_msg.ResponseCodes.BAD_REQUEST)
            
        log.info('Received addreslink; Title: ' + addresslink_msg.title)
        for person in addresslink_msg.person:
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
        """
        @brief Respond to a simple message
        @param params person_msg GPB, 20001/1, a person object from net.ooici.play.
        @retval response, GPB 20001/1, a person message if successful.
        """
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('hello_person', msg)
        
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def hello_everyone(self, msg):
        """
        @brief Respond to a simple message
        @param params addresslink_msg GPB, 20003/1, a addresslink object from net.ooici.play.
        @retval ack - a message envelope with ResponseCode OK and no content.
        """
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('hello_everyone', msg)
        
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(HelloMessage)


