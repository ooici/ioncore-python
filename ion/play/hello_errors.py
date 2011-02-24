#!/usr/bin/env python

"""
@file ion/play/hello_errors.py
@author David Stuebe
@brief Most services will have to handle possible exceptions in their business
logic. This is an example service that demostrates how to do that.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.exception import ApplicationError

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.messaging import message_client
from ion.core.object import object_utils
from ion.core.exception import ReceivedError

PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)


class HelloError(ApplicationError):
    """
    An Exception class for the hello errors example
    It inherits from the Application Error. Give a 'reason' and a 'response_code'
    when throwing a ApplicationError!
    """

class HelloErrors(ServiceProcess):
    """
    Example service interface that includes error handling
    This service has no purpose. It receives a person message and makes up some
    business logic based on the name of the person. The result, if any is also
    a person message. Normally the response would be a different type of object.
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_errors',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        log.info('HelloService.__init__()')

        ## you can either define your own message client or use the one defined
        ## for the process
        #self.mc = message_client.MessageClient(proc = self)


    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_replytome(self, request, headers, msg):
        log.info('op_replytome: '+str(request))
        
        response = yield self.businesslogic4replytome(request)

        # If no exception is raised, use reply okay and pass the response
        # reply is called once and only once no matter the result of the business logic!
        yield self.reply_ok(msg, content=response)

    @defer.inlineCallbacks # The business logic may involve defereds as well!
    def businesslogic4replytome(self, person_msg):
        """
        Determine how to respond to the message content
        """

        # Check the type of the content received
        if person_msg.MessageType != PERSON_TYPE:
            # Use the response code property of the person message.
            raise HelloErrors('Invalid message type recieved', person_msg.ResponseCodes.BAD_REQUEST)
            
            
        # Simplest example - Just reply okay with no object
        if person_msg.name == 'John Doe':
            # When reply_ok is called with None, an empty message is created which
            # includes a reply ok return code. 
            defer.returnValue(None)
            
        
        ####        
        # A message that succeeds - based on the name field of the person message
        ####
        if person_msg.name == 'Jane Doe':
            
            # Build a response message object        
            response = yield self.message_client.create_instance(PERSON_TYPE,MessageName='Example response message')
             # Business logic sets the value of the response
            response.name = 'Matthew'
            response.id = 8
            
            # Set the ResponseCode that we want to use for this result
            response.MessageResponseCode = response.ResponseCodes.ACCEPTED
            # A reason, if any for the result...
            response.MessageResponseBody = 'Jane is a nice person'
            
        ####
        # A message that fails in the application
        ####
        elif person_msg.name == """Robert); DROP TABLE Students;""":
            
            # This is an illegal request, Raise an Error.
            # Give a reason and a response code! (Get the response code from the person_msg message instance)
            raise HelloError('This operation faild due to bad request content.', person_msg.ResponseCodes.BAD_REQUEST)
            
                
        # An example of an uncaught exception
        else:
            
            # This is an uncaught exception - This should never happen!
            # In this case reply_err is called from the base process that dispatched the operation
            raise Exception("I'm an unexpected exception!")
            
            
        defer.returnValue(response)
        


class HelloErrorsClient(ServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello_errors"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def replytome(self, message):
        yield self._check_init()
            
        log.debug('Client Sending: '+str(message))
        (result, headers, msg) = yield self.rpc_send('replytome', message)
        
        log.debug('Service Replied: '+str(result))

        defer.returnValue(result)



# Spawn of the process using the module name
factory = ProcessFactory(HelloErrors)



