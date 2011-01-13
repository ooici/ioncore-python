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
    def test_hello(self):
            
        services = [
            {'name':'hello_my_error','module':'ion.play.hello_errors','class':'HelloErrors'},
        ]
            
        #Start the service
        sup = yield self._spawn_processes(services)
            
        # Create the client
        he = HelloErrorsClient(proc=sup)
            
        # Send a request - and it succeeds!
        response, result, ex = yield he.replytome("Succeed")
            
        log.info('Got Response: '+response) # Should always be a string - a defined response code!
        log.info('Got Result: '+str(result))
        log.info('Got Exception: '+ex)
            
        self.assertEqual(response,he.ION_SUCCESS)
        self.assertEqual(result,'Succeeded') # The response defined by the service as the content - can be anything that the interceptor knows how to send
        self.assertEqual(ex,'')
            
        # Send a request - and fail it!
        response, result, ex = yield he.replytome("Fail")
            
        log.info('Got Response: '+response) # Should always be a string - a defined response code!
        log.info('Got Result: '+str(result))
        log.info('Got Exception: '+ex)
            
        self.assertEqual(response,he.APP_FAILED)
        self.assertEqual(result,'Failed') # The response defined by the service as the content - can be anything that the interceptor knows how to send
        self.assertEqual(ex,'')
        
        
        # Send a request - and catch an exception
        response, result, ex = yield he.replytome("CatchMe")
            
        log.info('Got Response: '+response) # Should always be a string - a defined response code!
        log.info('Got Result: '+str(result))
        log.info('Got Exception: '+ex)
            
        self.assertEqual(response,he.APP_FAILED)
        self.assertEqual(result,'Caught') # The response defined by the service as the content - can be anything that the interceptor knows how to send
        self.assertEqual(ex,'''I'm supposed to fail''')
                
        # Send a request - and catch an exception
        try:
            response, result, ex = yield he.replytome("Can'tCatchMe")

        except ReceivedError, re: 
            # Why is re a list with a dictionary in it?
            response =  re[0]['response']
            result = None
            ex = re[0]['exception']
        
        # These do not get called - the receiver kills the container.
        # is that what we want?
            
        log.info('Got Response: '+response) # Should always be a string - a defined response code!
        log.info('Got Result: '+str(result))
        log.info('Got Exception: '+str(ex))
            
        self.assertEqual(response,he.ION_RECEIVER_ERROR)
        self.assertEqual(result,None) # The response defined by the service as the content - can be anything that the interceptor knows how to send
        self.assertEqual(ex,'''I'm an uncaught exception!''')
        
        
        
