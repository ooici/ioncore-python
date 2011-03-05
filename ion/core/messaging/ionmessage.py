#!/usr/bin/env python

"""
@file ion/core/intercept/ionmessage.py
@author Michael Meisinger
@brief base classes for the common ION message format
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.intercept.interceptor import EnvelopeInterceptor
import ion.util.procutils as pu


class IONMessageInterceptor(EnvelopeInterceptor):
    """
    Interceptor that assembles the headers in the ION message format.
    """
    def before(self, invocation):
        return invocation

    def after(self, invocation):
        message = invocation.message
        headers = message['headers']
        msg = {}
        # The following headers are FIPA ACL Message Format based
        # Exchange name of sender (DO NOT SEND replies here)
        msg['sender'] = str(headers.get('sender', message.get('sender')))
        # Exchange name of message recipient
        msg['receiver'] = str(message.get('recipient'))
        # Exchange name for message replies
        msg['reply-to'] = str(headers.get('reply-to', message.get('sender')))
        # Wire form encoding, such as 'json', 'fudge', 'XDR', 'XML', 'custom'
        msg['encoding'] = headers.get('encoding','json')
        # See ion.data.dataobject Serializers for choices
        msg['accept-encoding'] = headers.get('accept-encoding','')
        # Language of the format specification
        msg['language'] = headers.get('language','ion1')
        # Identifier of a registered format specification (i.e. message schema)
        msg['format'] = headers.get('format','raw')
        # Ontology associated with the content of the message
        msg['ontology'] = headers.get('ontology','')
        # OOI User id
        msg['user-id'] = str(headers.get('user-id', 'ANONYMOUS'))
        # Lifespan of user authority
        msg['expiry'] = str(headers.get('expiry', '0'))
        # Conversation instance id
        msg['conv-id'] = headers.get('conv-id','')
        # Conversation message sequence number
        msg['conv-seq'] = headers.get('conv-seq',1)
        # Conversation type id
        msg['protocol'] = headers.get('protocol','')
        # Status code
        msg['status'] = headers.get('status','OK')
        # Local timestamp in ms
        msg['ts'] = str(pu.currenttime_ms())
        #msg['reply-with'] = ''
        #msg['in-reply-to'] = ''
        #msg['reply-by'] = ''
        msg.update(headers)
        # Operation of the message, aka performative, verb, method
        msg['op'] = message.get('operation')
        # The actual content
        msg['content'] = message.get('content')

        invocation.message = msg
        return invocation
