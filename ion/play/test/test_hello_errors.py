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


class HelloErrorsBusinessLogicTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        
        # Starting the container is required! That way you can use the test supervisor process
        yield self._start_container()

        self.he = HelloErrors()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello_success(self):
                        
        # Use the convience method of the test case to create a message instance
        success = yield self.CreateMessage(MessageName='Succeed')
        result = yield self.he.businesslogic4replytome(success)
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.SUCCESS)
        
    @defer.inlineCallbacks
    def test_hello_fail(self):
                        
        # Use the convience method of the test case to create a message instance
        success = yield self.CreateMessage(MessageName='Fail')
        result = yield self.he.businesslogic4replytome(success)
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)

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
            
        # Create a mesasge client
        self.mc = message_client.MessageClient(proc=self.test_sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello_success(self):
            
        # Use the message client to create a message object
        # We are using the name to pass simple string arguments to the service
        # A real message should be created with a type and the content passed inside the message
        success = yield self.mc.create_instance(MessageName="Succeed")
            
        # Send a request - and succeeds!
        result = yield self.he.replytome(success)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.SUCCESS)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.OK) 
        self.assertEqual(result.MessageException,'')
          
          
    @defer.inlineCallbacks
    def test_hello_ok(self):
        # Send a request - and reply ok (no content)!
        ok = yield self.mc.create_instance(MessageName="OK")
            
        # Send a request - and succeeds!
        result = yield self.he.replytome(ok)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        # The Application Result is automagically set to success!
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.SUCCESS)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.OK) 
        self.assertEqual(result.MessageException,'')
          
    @defer.inlineCallbacks
    def test_hello_failure(self):
        # Send a request - and fail!
        fail = yield self.mc.create_instance(MessageName="Fail")
        result = yield self.he.replytome(fail)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.OK) 
        self.assertEqual(result.MessageException,'')
          
    @defer.inlineCallbacks
    def test_hello_error_reply_ok(self):
        # Send a request - and catch an exception. Reply Ok!
        catchme_ok = yield self.mc.create_instance(MessageName="CatchMe_OK")
        result = yield self.he.replytome(catchme_ok)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.OK) 
        self.assertEqual(result.MessageException,"I'm supposed to fail and reply_ok")
          
    @defer.inlineCallbacks
    def test_hello_error_reply_error(self):
        # Send a request - and catch an exception. Reply Err!
        catchme_err = yield self.mc.create_instance(MessageName="CatchMe_ERR")
        result = yield self.he.replytome(catchme_err)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.INTERNAL_ERROR) 
        self.assertEqual(result.MessageException,"I'm supposed to fail and reply_err")
        
        
    @defer.inlineCallbacks
    def test_hello_uncaught_error(self):
        ## Send a request - and catch an exception
        uncaught = yield self.mc.create_instance(MessageName="Can'tCatchMe")
        result = yield self.he.replytome(uncaught)
        
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.INTERNAL_ERROR) 
        self.assertEqual(result.MessageException,"I'm an uncaught exception!")
        
        