#!/usr/bin/env python

"""
@file ion/play/test/test_hello_errors.py
@test ion.play.hello_service Example unit tests for sample code.
@author Michael Meisinger
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.play.hello_errors import HelloErrorsClient, HelloErrors
from ion.test.iontest import IonTestCase
from ion.core.exception import ReceivedError

from ion.core.messaging import message_client

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

        self.hello_errors_backend = HelloErrors()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello_ok(self):
                        
        # Use the convience method of the test case to create a message instance
        request = yield self.CreateMessage(object_id=person_type, MessageName='Hello Error Message')
        # Use the long hand method to set fields in the message instance
        request.name = 'John Doe'
        request.id = 69 # This is just a field in the person object it has no significance in ION
        request.phone.add()
        request.phone.type = request.PhoneType.WORK
        request.phone.number = '123 456 7890'
        
        result = yield self.hello_errors_backend.businesslogic4replytome(request)
        
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.OK)
        
    @defer.inlineCallbacks
    def test_hello_success(self):
                        
        # Use the convience method of the test case to create a message instance
        success = yield self.CreateMessage(MessageName='Fail')
        result = yield self.hello_errors_backend.businesslogic4replytome(success)
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.BAD_REQUEST)
        
    @defer.inlineCallbacks
    def test_hello_fail(self):
                        
        # Use the convience method of the test case to create a message instance
        # set little johny droptables name using the kwargs in the convience method
        success = yield self.CreateMessage(person_type, MessageName='Hello Error Message', name="""Robert); DROP TABLE Students;""")
        result = yield self.hello_errors_backend.businesslogic4replytome(success)
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.BAD_REQUEST)
        
        
    

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
        self.he = HelloErrorsClient(proc=sup)
            
        ## You can create a message client or use the one built into the IonTest
        #self.mc = message_client.MessageClient(proc=self.test_sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello_success(self):
            
        # Use the message client to create a message object
        # We are using the name to pass simple string arguments to the service
        # A real message should be created with a type and the content passed inside the message
        success = yield self.CreateMessage(MessageName="Succeed")
            
        # Send a request - and succeeds!
        result = yield self.he.replytome(success)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageResponseCode))
        log.info('Got Exception: '+str(result.MessageResponseBody))
            
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.OK)
        self.assertEqual(result.MessageResponseBody,'')
          
          
    @defer.inlineCallbacks
    def test_hello_ok(self):
        # Send a request - and reply ok (no content)!
        ok = yield self.CreateMessage(MessageName="OK")
            
        # Send a request - and succeeds!
        result = yield self.he.replytome(ok)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageResponseCode))
        log.info('Got Exception: '+str(result.MessageResponseBody))
            
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.OK)
        self.assertEqual(result.MessageResponseBody,'')
          
    @defer.inlineCallbacks
    def test_hello_failure(self):
        # Send a request - and fail!
        fail = yield self.CreateMessage(MessageName="Fail")
        result = yield self.he.replytome(fail)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageResponseCode))
        log.info('Got Exception: '+str(result.MessageResponseBody))
            
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.BAD_REQUEST)
        self.assertEqual(result.MessageResponseBody,'')
          
    @defer.inlineCallbacks
    def test_hello_error_reply_ok(self):
        # Send a request - and catch an exception. Reply Ok!
        catchme_ok = yield self.CreateMessage(MessageName="CatchMe_OK")
        result = yield self.he.replytome(catchme_ok)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageResponseCode))
        log.info('Got Exception: '+str(result.MessageResponseBody))
            
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.BAD_REQUEST)
        self.assertEqual(result.MessageResponseBody,"I'm supposed to fail and reply_ok")
          
    @defer.inlineCallbacks
    def test_hello_error_reply_error(self):
        # Send a request - and catch an exception. Reply Err!
        catchme_err = yield self.CreateMessage(MessageName="CatchMe_ERR")
            
        try:
            result = yield self.he.replytome(catchme_err)
        except ReceivedError, re:
            # Catch the error raised by the receipt of an error message.
            log.debug('ReceivedError', str(re))
            content = re[1]
            
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageResponseCode))
        log.info('Got Exception: '+str(result.MessageResponseBody))
            
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.INTERNAL_SERVER_ERROR)
        self.assertEqual(result.MessageResponseBody,"I'm supposed to fail and reply_err")
        
        
    @defer.inlineCallbacks
    def test_hello_uncaught_error(self):
        ## Send a request - and catch an exception
        uncaught = yield self.CreateMessage(MessageName="Can'tCatchMe")
        result = yield self.he.replytome(uncaught)
        
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageResponseCode))
        log.info('Got Exception: '+str(result.MessageResponseBody))
            
        self.assertEqual(result.MessageResponseCode,result.ResponseCodes.INTERNAL_SERVER_ERROR)
        self.assertEqual(result.MessageResponseBody,"I'm an uncaught exception!")
        
        