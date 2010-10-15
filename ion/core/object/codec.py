#!/usr/bin/env python

"""
@file ion/core/object/codec.py
@author David Stuebe
@brief Interceptor for encoding and decoding ION messages
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.intercept.interceptor import EnvelopeInterceptor
import ion.util.procutils as pu

from ion.core.object import gpb_wrapper


class ObjectCodecInterceptor(EnvelopeInterceptor):
    """
    Interceptor that assembles the headers in the ION message format.
    """
    def before(self, invocation):
        print 'Hello from BEFORE Davids Codec!'
        print 'args', invocation.args
        print 'path', invocation.path
        print 'message', invocation.message
        print 'content', invocation.content
        print 'status', invocation.status
        print 'route', invocation.route
        print 'note', invocation.note
    
        
        return invocation

    def after(self, invocation):
        print 'Hello from AFTER Davids Codec!'
        print 'args', invocation.args
        print 'path', invocation.path
        print 'message', invocation.message
        print 'content', invocation.content
        print 'status', invocation.status
        print 'route', invocation.route
        print 'note', invocation.note
        
        content = invocation.message['content']
        print 'DNMDNDNDNDNDNDND', content, type(content)
        if isinstance(content, gpb_wrapper.Wrapper):
            print 'EJHEEHEHEHEHEHEHEHEHEHEHEEHHE'
            invocation.message['content']= content.SerializeToString()

        
        
        return invocation
