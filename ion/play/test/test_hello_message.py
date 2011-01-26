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
from ion.core.process.process import Process, ProcessClient, ProcessDesc
from ion.core import bootstrap

from ion.core.object import object_utils

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)


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
        
        # Use the message client to create a message object
        ab_msg = yield mc.create_instance(addresslink_type, name='addressbook message')
        
        #ab is a message instance of type addresslink 
        ab_msg.title = 'An addressbook object for testing'
        
        # Add a new entry in the list (repeated) persons of the addressbook
        ab_msg.person.add()
    
        # Make a new person object to go in the list
        person = ab_msg.CreateObject(person_type)
        person.name = 'david'
        person.id = 59
        person.email = 'stringgggg'
        ph = person.phone.add()
        ph.number = '401 789 6224'
        
        # Since ab.person is of type 'link' we need to set the link equal to the
        # person object
        ab_msg.person[0] = person
        
        log.info('AdressBook!' + str(ab_msg))        
        
        # Lets try sending the addressbook now...
        log.info('Calling hello everyone...')
        yield hc1.hello_everyone(ab_msg)
        # The response is just a ack
        
        # Now try making a person object and using the person from the ab message
        # You can move objects from one place to another...
        
        person_msg = yield mc.create_instance(person_type, name='my message')
        
        person_msg.MessageObject = ab_msg.person[0]
        
        log.info('Calling hello person...')
        # You can send the root of the object or any linked composite part of it.
        result_person_msg = yield hc1.hello_person(person_msg)
        
        # This is a different repository from the one we started with!
        log.info('Nome of the command object:' + person_msg.Repository.repository_key)
        log.info('Nome of the response object:' + result_person_msg.Repository.repository_key)
        
        
        # Now lets add the new_person from the reply to the addressbook        
        p2 = ab_msg.person.add()
        
        # You can move a linked object from one repository to another when it
        # has been commited. An exception will be raised if you try and move
        # a modified object. Only a linked composite can be moved
        #p2.SetLink(new_person)
        # you can use an assignment operator or the set link command...
        ab_msg.person[1] = result_person_msg.MessageObject
        
        # The address book now has two perosn object, but you only see the links in the log
        log.info('Print the addressbook after adding the respoonse: \n' + str(ab_msg))
        
        # Lets try sending the addressbook again...
        log.info('Calling hello everyone...')
        yield hc1.hello_everyone(ab_msg)
        
        log.info('Tada!')
        
        
        
        