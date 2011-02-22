#!/usr/bin/env python

"""
@file ion/play/test/test_hello_object.py
@test ion.play.hello_object Example unit tests for sample code.
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.test.iontest import IonTestCase, ReceiverProcess

from ion.core.process.process import Process, ProcessClient, ProcessDesc
from ion.core import bootstrap

from ion.core.object import object_utils
from ion.core.object import workbench
from ion.core.messaging import message_client

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)


class MessageClientTest(IonTestCase):
    """
    Testing example hello object service.
    This example shows how it is possible to create and send strongly typed objects
    Each time an object is sent it is assigned a new identifier. The example
    shows how it is possible to move a linked composite from one object to another.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_message_client(self):
        
        # Create a mesasge client
        mc = message_client.MessageClient(proc=self.test_sup)
        
        message = yield mc.create_instance(person_type, MessageName='person message')
        
        # test the name property        
        self.assertEqual(message.MessageName, 'person message')
        message.MessageName = 'My message'
        self.assertEqual(message.MessageName, 'My message')
        
        # Test the id property
        self.assertEqual(message.MessageIdentity, message.Repository.repository_key)
        
        # Test the type property
        self.assertEqual(message.MessageType, person_type)
        
        # Test setting a field
        message.name = 'David'
        
        self.assertEqual(message.name, 'David')
        
        
    @defer.inlineCallbacks
    def test_send_message_instance(self):
        processes = [
            {'name':'echo1','module':'ion.core.process.test.test_process','class':'EchoProcess'},
        ]
        sup = yield self._spawn_processes(processes)
        pid = sup.get_child_id('echo1')
            
        mc = message_client.MessageClient(proc=self.test_sup)
            
        message = yield mc.create_instance(person_type, MessageName='person message')
        message.name ='David'
        
        (response, headers, msg) = yield self.test_sup.rpc_send(pid, 'echo', message)
                
        self.assertIsInstance(response, message_client.MessageInstance)
        self.assertEqual(response.name, 'David')
        
    
    @defer.inlineCallbacks
    def test_message_instance(self):
        
        # Create a mesasge client
        mc = message_client.MessageClient(proc=self.test_sup)
            
        # Test a message with no object
        message = yield mc.create_instance(None, MessageName='person message')
        
        # test the ResponseCodes property        
        self.assertEqual(message.ResponseCodes.OK, 200)
        
                
        # The message objects fields are not accessible...
        self.assertRaises(AttributeError, setattr, message, 'response_code', message.ResponseCodes.OK)
        
        # Except throught the the getter/setter properties
        message.MessageResponseCode = message.ResponseCodes.OK
        
        
        
class IonTestMessageClientTest(IonTestCase):
    """
    Testing example hello object service.
    This example shows how it is possible to create and send strongly typed objects
    Each time an object is sent it is assigned a new identifier. The example
    shows how it is possible to move a linked composite from one object to another.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_convience_methods(self):
        
        
        msg_instance = yield self.CreateMessage(person_type, MessageName='David', name='david', id=6)
        
        