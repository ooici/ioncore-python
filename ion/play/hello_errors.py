#!/usr/bin/env python

"""
@file ion/play/hello_errors.py
@author David Stuebe
@brief An example service that has error checking
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.messaging import message_client
from ion.core.object import object_utils
from ion.core.exception import ReceivedError

person_type = object_utils.create_type_identifier(object_id=20001, version=1)


class HelloError_ERR(Exception):
    """An Exception class for the hello errors example
    """

class HelloError_OK(Exception):
    """An Exception class for the hello errors example
    """

class HelloErrors(ServiceProcess):
    """
    Example service interface
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello_errors',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        log.info('HelloService.__init__()')

        self.mc = message_client.MessageClient(proc = self)

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_replytome(self, content, headers, msg):
        log.info('op_replytome: '+str(content))

        try:
            response = yield self.businesslogic4replytome(content)
        
        except HelloError_ERR, he:
            # Use replay error to pass the exception and raise a Reciever Error in the calling process
            
            yield self.reply_err(msg, exception=he)
            # reply_err will automatically create a response message if you don't provide one and put the passed exception in it.
            defer.returnValue(None)
            
        except HelloError_OK, he:
            # Use reply okay and pass the expection in a response object with the exception included.
            # The calling process must process the response to determine what to do about it...

            # Create the response - an empty message and put the exception in it.
            response = yield self.mc.create_instance(name='Example message')
            response.MessageApplicationResponse = response.ApplicationResponse.FAILED
            response.MessageException = str(he)
            
            yield self.reply_ok(msg, content=response)
            defer.returnValue(None)

        # If no exception is raised, use reply okay and pass the response
        # reply is called once and only once no matter the result of the business logic!
        yield self.reply_ok(msg, content=response)

    @defer.inlineCallbacks # The business logic may involve defereds as well!
    def businesslogic4replytome(self,content):
        """
        Determine how to respond to the message content
        May include headers or even message as part of the business logic if needed
        """
        
        
        ####        
        # A message that succeeds
        ####
        if content.MessageName == 'Succeed':
            
            # Create a message to contain the response...
            response = yield self.mc.create_instance(person_type,name='Example message')
            # Set response values here... using a person object as an example
            response.name = 'Matthew'
            response.id = 8
            
            response.MessageApplicationResponse = response.ApplicationResponse.SUCCESS
        
        # Let the service just reply okay with no value!
        elif content.MessageName == 'OK':
            response = None
        
        ####
        # A message that fails in the application - with no exception.
        ####
        elif content.MessageName == 'Fail':
            
            # Create a message to contain the response... it may or may not have a type beyond the basic message container
            response = yield self.mc.create_instance(name='Example "empty" failure message')
            
            # The person object fields are not set.... the the type of the object
            response.MessageApplicationResponse = response.ApplicationResponse.FAILED
            
        # A message that fails generating an exception 
        elif content.MessageName == 'CatchMe_OK':
                
            raise HelloError_OK("I'm supposed to fail and reply_ok")
                
        elif content.MessageName == 'CatchMe_ERR':
            
            raise HelloError_ERR("I'm supposed to fail and reply_err")
            
            
        # An example of an uncaught exception - this does not yet work as I expect
        else:
            # This is an uncaught exception
            # we want to make sure that reply is not called from the application
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
    def replytome(self, text='Hi there'):
        yield self._check_init()
        
        try:
            (content, headers, msg) = yield self.rpc_send('replytome', text)
        except ReceivedError, re:
            
            log.debug('ReceivedError', str(re))
            content = re[1]
            
        
        log.debug('Service reply: '+str(content))

        defer.returnValue(content)



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
