#!/usr/bin/env python

"""
@file ion/core/exception.py
@author Michael Meisinger
@brief module for exceptions
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class IonError(StandardError):
    pass

    # @todo Some better str output

class ConfigurationError(IonError):
    pass

class StartupError(IonError):
    pass

class IllegalStateError(IonError):
    pass

class ApplicationError(IonError):
    """
    @Brief An Exception class for use in service business logic which will not result in the service
    being terminated. Any exception thrown which is not a subclass of Application Error will
    cause the process to terminate.
    """
    
    def __init__(self, reason, response_code):
        """
        @param reason a string explaining the cause of the exception
        @param response_code is an http style numerical error code. These are defined in the ION Message
        When this exception is used in RPC messaging, a 400 level code will result in a
        ReceivedApplicationError in the originating process. A 500 level code will result in a
        ReceivedContainerError.
        """
        
        # Set up the exception
        IonError.__init__(self, reason)
        
        # Set the response code
        self.response_code = response_code


class ReceivedError(IonError):

    def __init__(self, headers, content):
            self.msg_headers = headers
            self.msg_content = content
            msg = "ERROR received in message"
            IonError.__init__(self, msg)

class ReceivedContainerError(ReceivedError):
    """
    An exception to throw when a 5XX response code is received during RPC messaging
    """
    
class ReceivedApplicationError(ReceivedError):
    """
    An exception to throw for 4XX response code is received during RPC messaging
    """



#class ReceivedError(IonError):
#
#    def __init__(self, *args, **kwargs):
#        if len(args) == 2 and type(args[0]) is dict and type(args[1]) is dict:
#            headers = args[0]
#            content = args[1]
#            self.msg_headers = headers
#            self.msg_content = content
#            msg = content.get('errmsg', "ERROR received in message")
#            IonError.__init__(self, msg)
#        else:
#            IonError.__init__(self, *args, **kwargs)
