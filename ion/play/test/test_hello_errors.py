#!/usr/bin/env python

"""
@file ion/play/test/test_hello_errors.py
@test ion.play.hello_service Example unit tests for sample code.
@author Michael Meisinger
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.play.hello_errors import HelloErrorsClient
from ion.test.iontest import IonTestCase
from ion.core.exception import ReceivedError
class HelloErrorsTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello_errors(self):
            
        services = [
            {'name':'hello_my_error','module':'ion.play.hello_errors','class':'HelloErrors'},
        ]
            
        #Start the service
        sup = yield self._spawn_processes(services)
            
        # Create the client to the hello errors service
        he = HelloErrorsClient(proc=sup)
            
        # Create a mesasge client
        mc = MessageClient(proc=self.test_sup)
        
        # Use the message client to create a message object
        # We are using the name to pass simple string arguments to the service
        # A real message should be created with a type and the content passed inside the message
        success = yield mc.create_instance(name="Succeed")
            
        # Send a request - and succeeds!
        result = yield he.replytome(success)
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.SUCCESS)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.OK) 
        self.assertEqual(result.MessageException,'')
          
          
        # Send a request - and reply ok (no content)!
        result = yield he.replytome("OK")
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        # The Application Result is automagically set to success!
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.SUCCESS)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.OK) 
        self.assertEqual(result.MessageException,'')
          
        # Send a request - and fail!
        result = yield he.replytome("Fail")
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.OK) 
        self.assertEqual(result.MessageException,'')
          
          
        # Send a request - and catch an exception. Reply Ok!
        result = yield he.replytome("CatchMe_OK")
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.OK) 
        self.assertEqual(result.MessageException,"I'm supposed to fail and reply_ok")
          
            
        # Send a request - and catch an exception. Reply Err!
        result = yield he.replytome("CatchMe_ERR")
            
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.INTERNAL_ERROR) 
        self.assertEqual(result.MessageException,"I'm supposed to fail and reply_err")
        
        
        
        ## Send a request - and catch an exception
        result = yield he.replytome("Can'tCatchMe")
        
        log.info('Got Response: '+str(result.MessageObject)) 
        log.info('Got Application Result: '+str(result.MessageApplicationResponse))
        log.info('Got ION Result: '+str(result.MessageIonResponse))
        log.info('Got Exception: '+str(result.MessageException))
            
        self.assertEqual(result.MessageApplicationResponse,result.ApplicationResponse.FAILED)
        self.assertEqual(result.MessageIonResponse,result.IonResponse.INTERNAL_ERROR) 
        self.assertEqual(result.MessageException,"I'm an uncaught exception!")
        
        