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

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_replytome(self, content, headers, msg):
        log.info('op_replytome: '+str(content))

        response, result, ex = self.businesslogic4replytome(content)
        #response, result, ex = yield self.businesslogic4replytome(content)

        # The following line shows how to reply to a message
        # reply is called once and only once no matter the result of the business logic!
        yield self.reply(msg, content=result, response_code=response, exception=ex)

    #@defer.inlineCallbacks # The business logic may involve defereds as well!
    def businesslogic4replytome(self,content):
        """
        Determine how to respond to the message content
        May include headers or even message as part of the business logic if needed
        """
        response = ''
        ex = ''
                
        # A message that succeeds
        if content == 'Succeed':
            result = 'Succeeded'

        # A message that fails in the application
        elif content == 'Fail':
            result = 'Failed'
            response = self.APP_FAILED
            
        # A message that fails generating an exception 
        elif content == 'CatchMe':
            
            try:
                # Some problem here!
                raise RuntimeError("I'm supposed to fail")
                result = 'Succeeded'

            except RuntimeError as re:
            
                result = 'Caught'
                response = self.APP_FAILED
                ex = re

        # An example of an uncaught exception - this does not yet work as I expect
        else:
            # This is an uncaught exception
            # we want to make sure that reply is not called from the application
            raise RuntimeError("I'm an uncaught exception!")
            
            # These are set automatically when the exception is handled in the
            # infrastructure!
            result = 'Should not be returned!'
            response = 'Should not be returned either!'
            ex = 'This one too!'
            
        
        # if the business logic involves defereds you need to change the return
        #defer.returnValue((response, result, exception))
        return response, result, ex
        
        


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
        (content, headers, msg) = yield self.rpc_send('replytome', text)
        log.info('Service reply: '+str(content))

        result = content
        response = headers.get(self.MSG_RESPONSE)
        exception = headers.get(self.MSG_EXCEPTION)

        defer.returnValue((response, result, exception))



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
