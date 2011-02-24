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
    
    def __init__(self, reason, response_code):
        
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
