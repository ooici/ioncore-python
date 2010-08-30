#!/usr/bin/env python

"""
@file ion/interact/message.py
@author Michael Meisinger
@brief classes for managing messages in the standard ION format
"""

import logging
log = logging.getLogger(__name__)

class Message(object):
    """
    This class represents a message received via a process and to be sent via
    a process
    """
    def __init__(self, content, headers=None):
        self.content = content
        self.headers = headers
    
    @classmethod
    def from_carrotmsg(cls, msg):
        """
        Factory method for constructing a Message instance from a (previously
        received) carrot message.
        """
        # The following fails if msg is not a proper carrot BaseMessage
        payload = msg.payload
        # The following fails if msg is not a proper standard message
        content = payload['content']
        inst = cls(content, payload)
        inst.msg = msg
        return inst
