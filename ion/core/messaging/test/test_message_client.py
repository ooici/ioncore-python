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
from twisted.trial import unittest

from ion.core.object import object_utils
from ion.core.object import workbench
from ion.core.messaging import message_client

ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)
ION_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=11, version=1)


class MessageInstanceTest(unittest.TestCase):

    
    def setUp(self):
        
        # Fake what the message client does to create a message
        self.wb = workbench.WorkBench('No Process')
        
        msg_repo = self.wb.create_repository(ION_MESSAGE_TYPE)
        msg_object = msg_repo.root_object
        msg_object.identity = msg_repo.repository_key

        print dir(msg_object.message_object)

        msg_object.message_object = msg_repo.create_object(ADDRESSLINK_TYPE)

        msg_repo.commit('Message object instantiated')

        self.msg = message_client.MessageInstance(msg_repo)


    def test_listsetfields(self):
        """ Testing for this Method is more through in the wrapper test
        """


        self.msg.title = 'foobar'

        flist = self.msg.ListSetFields()
        self.assertIn('title',flist)

        self.msg.owner = self.msg.CreateObject(PERSON_TYPE)

        flist = self.msg.ListSetFields()
        self.assertIn('title',flist)
        self.assertIn('owner',flist)

        self.msg.person.add()
        self.msg.person[0] = self.msg.CreateObject(PERSON_TYPE)

        flist = self.msg.ListSetFields()
        self.assertIn('title',flist)
        self.assertIn('owner',flist)
        self.assertIn('person',flist)

        self.assertEqual(len(flist),3)

    def test_isfieldset(self):
        """ Testing for this Method is more through in the wrapper test
        """
        self.assertEqual(self.msg.IsFieldSet('title'),False)
        self.msg.title = 'foobar'
        self.assertEqual(self.msg.IsFieldSet('title'),True)

        self.assertEqual(self.msg.IsFieldSet('owner'),False)
        self.msg.owner = self.msg.CreateObject(PERSON_TYPE)
        self.assertEqual(self.msg.IsFieldSet('owner'),True)


        self.assertEqual(self.msg.IsFieldSet('person'),False)
        self.msg.person.add()
        self.assertEqual(self.msg.IsFieldSet('person'),False)
        self.msg.person[0] = self.msg.CreateObject(PERSON_TYPE)
        self.assertEqual(self.msg.IsFieldSet('person'),True)




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
        
        message = yield mc.create_instance(PERSON_TYPE, MessageName='person message')
        
        # test the name property        
        self.assertEqual(message.MessageName, 'person message')
        message.MessageName = 'My message'
        self.assertEqual(message.MessageName, 'My message')
        
        # Test the id property
        self.assertEqual(message.MessageIdentity, message.Repository.repository_key)
        
        # Test the type property
        self.assertEqual(message.MessageType, PERSON_TYPE)
        
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
            
        message = yield mc.create_instance(PERSON_TYPE, MessageName='person message')
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
        
        
        msg_instance = yield self.create_message(PERSON_TYPE, MessageName='David', name='david', id=6)
        
        