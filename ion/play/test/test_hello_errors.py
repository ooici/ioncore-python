#!/usr/bin/env python

"""
@file ion/play/test/test_hello_errors.py
@test ion.play.hello_service Example unit tests for sample code.
@author Michael Meisinger
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.play.hello_errors import HelloErrorsClient, HelloErrors, HelloError
from ion.test.iontest import IonTestCase
from ion.core.exception import ReceivedError, ReceivedApplicationError, ReceivedContainerError

from ion.core.messaging import message_client
from ion.core.object import object_utils

# Use the person object as a simple type of message content
person_type = object_utils.create_type_identifier(object_id=20001, version=1)


class HelloErrorsBusinessLogicTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        
        # Starting the container is required! That way you can use the test supervisor process
        yield self._start_container()
        
        # Create an instance of the service process we want to test
        self.hello_errors_backend = HelloErrors()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

        
    @defer.inlineCallbacks
    def test_hello_accept(self):
                        
        # Use the convience method of the test case to create a message instance
        request = yield self.create_message(object_id=person_type, MessageName='Hello Error Message')
        request.name = 'Jane Doe'
        request.id = 42 # This is just a field in the person object it has no significance in ION
        request.phone.add()
        request.phone.type = request.PhoneType.WORK
        request.phone.number = '123 456 7890'
        
        # Pass the request - as though it were sent by the messaging to the business logic
        result = yield self.hello_errors_backend.businesslogic4replytome(request)
        
        # Check the response of the result message
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.ACCEPTED)
        self.assertEqual(result.MessageResponseBody, 'Jane is a nice person')

        # Check the content of the result message
        self.assertEqual(result.name, 'Matthew')        
        
    @defer.inlineCallbacks
    def test_hello_fail(self):
                        
        # set little johny droptables name using the kwargs in the convience method
        request = yield self.create_message(person_type, MessageName='Hello Error Message', name="""Robert); DROP TABLE Students;""")
        # Don't bother setting other fields...
        
        try:
            result = yield self.hello_errors_backend.businesslogic4replytome(request)
        except HelloError, ex:
            
            # Check the response of the result message
            self.assertEqual(ex.response_code,request.ResponseCodes.BAD_REQUEST)
            self.assertEqual(str(ex), 'This operation faild due to bad request content.')
            defer.returnValue(True)
        
        self.fail()

    
    @defer.inlineCallbacks
    def test_hello_ok(self):
                        
        # Use the convience method of the test case to create a message instance
        # Keyword arguments can be used to set simple fields in the message
        request = yield self.create_message(object_id=person_type, MessageName='Hello Error Message', name='John Doe', id=42)
        # Use the long hand method to set fields in the message instance
        request.phone.add()
        request.phone.type = request.PhoneType.WORK
        request.phone.number = '123 456 7890'
        
        result = yield self.hello_errors_backend.businesslogic4replytome(request)
        
        # In case of a Ack, the business logic just returns None as the result
        # in the service test we will see that this becomes a OK message.
        self.assertEqual(result, None)

class HelloErrorsTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'hello_my_error','module':'ion.play.hello_errors','class':'HelloErrors'},
        ]
            
        #Start the service
        sup = yield self._spawn_processes(services)
            
        # Create the client to the hello errors service
        self.hello_errors_client = HelloErrorsClient(proc=sup)
            
        ## You can create a message client or use the one built into the IonTest
        #self.mc = message_client.MessageClient(proc=self.test_sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello_ok(self):
            
        # Create the same message we passed to the business logic test
        request = yield self.create_message(object_id=person_type, MessageName='Hello Error Message', name='John Doe', id=42)
        # Use the long hand method to set fields in the message instance
        request.phone.add()
        request.phone.type = request.PhoneType.WORK
        request.phone.number = '123 456 7890'

        # Send a request - and succeeds!
        result = yield self.hello_errors_client.replytome(request)
            
        # We got back a message instance object - reply OK automatically makes one.
        self.assertIsInstance(result, message_client.MessageInstance)
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.OK)
        self.assertEqual(result.MessageResponseBody,'')
        self.assertEqual(result.MessageType, None)
          
          
    @defer.inlineCallbacks
    def test_hello_accept(self):
        
        # Create the same message we passed to the business logic test
        request = yield self.create_message(object_id=person_type, MessageName='Hello Error Message')
        request.name = 'Jane Doe'
        request.id = 42 # This is just a field in the person object it has no significance in ION
        request.phone.add()
        request.phone.type = request.PhoneType.WORK
        request.phone.number = '123 456 7890'
        
        # Send a request - and succeeds!
        result = yield self.hello_errors_client.replytome(request)
            
        # Some asserts about the result message
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.ACCEPTED)
        self.assertEqual(result.MessageResponseBody,'Jane is a nice person')
        self.assertEqual(result.MessageType, person_type)
        self.assertEqual(result.name, 'Matthew')
        
          
    @defer.inlineCallbacks
    def test_hello_failure(self):
        
        # set little johny droptables name using the kwargs in the convience method
        request = yield self.create_message(person_type, MessageName='Hello Error Message', name="""Robert); DROP TABLE Students;""")
        # Don't bother setting other fields...
        
        # Send a request - and catch the exception!
        try:
            result = yield self.hello_errors_client.replytome(request)
            
        except ReceivedApplicationError, ex:
            ex_msg = ex.msg_content
            msg_headers = ex.msg_headers
            
        self.assertEqual(ex_msg.MessageResponseCode,ex_msg.ResponseCodes.BAD_REQUEST)
        self.assertEqual(ex_msg.MessageResponseBody,'This operation faild due to bad request content.')
        
          
    @defer.inlineCallbacks
    def test_hello_error(self):
        # Send a request - and raise an uncaught exception
        request = yield self.create_message(person_type, MessageName="Hello Error Message", name="Raise an uncaught exception")

        try:
            result = yield self.hello_errors_client.replytome(request)
            
        except ReceivedContainerError, ex:
            ex_msg = ex.msg_content
            msg_headers = ex.msg_headers
            
        self.assertEqual(ex_msg.MessageResponseCode, ex_msg.ResponseCodes.INTERNAL_SERVER_ERROR)
        self.assertEqual(ex_msg.MessageResponseBody, "I'm an unexpected exception!")
          
          
          
    @defer.inlineCallbacks
    def test_hello_error_and_recover(self):
        # set little johny droptables name using the kwargs in the convience method
        request = yield self.create_message(person_type, MessageName='Hello Error Message', name="""Robert); DROP TABLE Students;""")
        # Don't bother setting other fields...
        
        # Send a request - and catch the exception!
        try:
            result = yield self.hello_errors_client.replytome(request)
            
        except ReceivedApplicationError, ex:
            ex_msg = ex.msg_content
            msg_headers = ex.msg_headers
            
        self.assertEqual(ex_msg.MessageResponseCode,ex_msg.ResponseCodes.BAD_REQUEST)
        self.assertEqual(ex_msg.MessageResponseBody,'This operation faild due to bad request content.')
        
        
        # Now send another message - the service is still active!
            
        # Create the same message we passed to the business logic test
        request = yield self.create_message(object_id=person_type, MessageName='Hello Error Message')
        request.name = 'Jane Doe'
        request.id = 42 # This is just a field in the person object it has no significance in ION
        request.phone.add()
        request.phone.type = request.PhoneType.WORK
        request.phone.number = '123 456 7890'
        
        # Send a request - and succeeds!
        result = yield self.hello_errors_client.replytome(request)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageResponseCode))
        log.info('Got Exception: '+str(result.MessageResponseBody))
            
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.ACCEPTED)
        self.assertEqual(result.MessageResponseBody,'Jane is a nice person')
        self.assertEqual(result.MessageType, person_type)
        self.assertEqual(result.name, 'Matthew')
    
    
    
    
    