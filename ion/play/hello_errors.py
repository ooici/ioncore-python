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

person_type = object_utils.create_type_identifier(object_id=20001, version=1)


class HelloError(ApplicationError):
    """
    An Exception class for the hello errors example
    It inherits from the Application Error. Give a 'reason' and a 'response_code'
    when throwing a ApplicationError!
    """

class HelloErrors(ServiceProcess):
    """
    Example service interface that includes error handleing
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
        log.info('op_replytome: '+str(content))
        
        response = yield self.businesslogic4replytome(request)

        # If no exception is raised, use reply okay and pass the response
        # reply is called once and only once no matter the result of the business logic!
        yield self.reply_ok(msg, content=response)

    @defer.inlineCallbacks # The business logic may involve defereds as well!
    def businesslogic4replytome(self, person_msg):
        """
        Determine how to respond to the message content
        May include headers or even message as part of the business logic if needed
        """

        #if person_msg.MessageType != person_type:
        #    raise HelloErrors('Invalid message type recieved')


        # Simplest example - Just reply okay with no object
        if response.name == 'John Doe':
            defer.returnValue(None)
            
        
        # Build a response message object        
        response = yield self.message_client.create_instance(person_type,MessageName='Example response message')
        # Set response values here... using a person object as an example
        response.name = 'Matthew'
        response.id = 8
        
        ####        
        # A message that succeeds
        ####
        if request.MessageName == 'Succeed':
            
            # Create a message to contain the response...
            response.MessageResponseCode = response.ResponseCodes.OK
            
        ####
        # A message that fails in the application
        ####
        elif request.MessageName == 'Fail':
            
            # The person object fields are not set.... the the type of the object
            raise HelloError('This is a failed operation', response.ResponseCodes.BAD_REQUEST)
            
        # A message that fails generating an exception 
        elif request.MessageName == 'CatchMe_OK':
                
            try:
                # Do some business logic that raises an exception
                
                bad = {'value':5}
                response.name = bad['not here']
                
            except KeyError, ex:
                
                # Use reply okay and pass the expection in a response object with the exception included.
                # The calling process must process the response to determine what to do about it...
                
                raise HelloError('Caught an exception in the logic and raised the service error', response.ResponseCodes.NOT_FOUND)
                
        # An example of an uncaught exception
        else:
            
            # This is an uncaught exception - This should never happen!
            # In this case reply_err is called from the base process that dispatched the operation
            raise RuntimeError("I'm an uncaught exception!")
            
            
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



"""
from ion.play import hello_service as h
spawn(h)

# TODO:
# supervisor process is #1, our hello_service should be #2.
# sending to 1 results in a callback with no data, sending to 2 does not
# ever call back.
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient()
hc.hello()
"""
