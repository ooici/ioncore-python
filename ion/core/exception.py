#!/usr/bin/env python

"""
@file ion/core/exception.py
@author Michael Meisinger
@brief module for exceptions
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class IONError(StandardError):
    pass

    # @todo Some better str output

class ConfigurationError(IONError):
    pass

class StartupError(IONError):
    pass

class IllegalStateError(IONError):
    pass

class ConversationError(IONError):
    pass

class ReceivedError(IONError):

    def __init__(self, *args, **kwargs):
        if len(args) == 2 and type(args[0]) is dict and type(args[1]) is dict:
            headers = args[0]
            content = args[1]
            self.msg_headers = headers
            self.msg_content = content
            msg = content.get('errmsg', "ERROR received in message")
            IONError.__init__(self, msg)
        else:
            IONError.__init__(self, *args, **kwargs)
