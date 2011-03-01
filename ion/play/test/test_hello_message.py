#!/usr/bin/env python

"""
@file ion/play/test/test_hello_message.py
@test ion.play.hello_message Example unit tests for sample code.
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.play.hello_message import HelloMessageClient
from ion.test.iontest import IonTestCase
from ion.core.messaging.message_client import MessageClient
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError


from ion.core.object import object_utils

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

class HelloMessageTest(IonTestCase):
    """
    Testing example hello message service.
    This example shows how it is possible to create and send strongly typed messages
    Each time a message is sent it is assigned a new identifier. The example
    shows how it is possible to move a linked composite from one message to another.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_hello(self):
            
        services = [
            {'name':'hello1','module':'ion.play.hello_message','class':'HelloMessage'},
        ]

        sup = yield self._spawn_processes(services)
        
        # Create a hello object client
        hc1 = HelloMessageClient(proc=self.test_sup)
            
        # Create a mesasge client
        mc = MessageClient(proc=self.test_sup)
        # There is also a message client built into the IonTestCase object
        
        # Use the message client to create a message object
        ab_msg = yield mc.create_instance(ADDRESSLINK_TYPE, MessageName='addressbook message')
        
        #ab is a message instance of type addresslink 
        ab_msg.title = 'An addressbook object for testing'
        
        # Add a new entry in the list (repeated) persons of the addressbook
        ab_msg.person.add()
    
        # Make a new person object to go in the list
        ab_msg.person[0] = ab_msg.CreateObject(PERSON_TYPE)
        ab_msg.person[0].name = 'david'
        ab_msg.person[0].id = 59
        ab_msg.person[0].email = 'stringgggg'
        ab_msg.person[0].phone.add()
        ab_msg.person[0].phone[0].number = '401 789 6224'
        
        log.info('AdressBook! \n' + str(ab_msg))        
        
        # Lets try sending the addressbook now...
        log.info('Calling hello everyone...')
        yield hc1.hello_everyone(ab_msg)
        # The response is just a ack
        
        ### Now try making a person object and using the person from the ab message
        # You can move objects from one place to another...
        person_msg = yield mc.create_instance(MessageContentTypeID=PERSON_TYPE)
        
        # Use the person we made from the first message
        person_msg.MessageObject = ab_msg.person[0]
        
        log.info('Calling hello person...')
        # You can send the root of the object or any linked composite part of it.
        result_person_msg = yield hc1.hello_person(person_msg)
        
        
        # Now lets add the new_person from the reply to the addressbook        
        ab_msg.person.add()
        
        # You can move a linked object from one repository to another when it
        # has been commited. An exception will be raised if you try and move
        # a modified object. Only a linked composite can be moved
        
        # you can use an assignment operator or the set link command...
        # ab_msg.person.SetLink(1,result_person_msg.MessageObject)
        ab_msg.person[1] = result_person_msg.MessageObject
        
        # The address book now has two perosn object, but you only see the links in the log
        log.info('Print the addressbook after adding the respoonse: \n' + str(ab_msg))
        
        # Lets try sending the addressbook again...
        log.info('Calling hello everyone...')
        yield hc1.hello_everyone(ab_msg)
        
        log.info('Tada!')
        
        
        
        