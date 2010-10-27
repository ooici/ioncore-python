#!/usr/bin/env python

"""
@file ion/core/intercept/interceptor.py
@author Michael Meisinger
@brief capability container interceptor system (security, governance)
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.process.cprocess import ContainerProcess, Invocation
from ion.core.exception import ConfigurationError
import ion.util.procutils as pu

# Configuration
CONF = ioninit.config(__name__)
msg_sign = CONF.getValue('msg_sign', True)


class Interceptor(ContainerProcess):
    """
    Interceptor that processes messages as the come along and passes them on.
    """

class EnvelopeInterceptor(Interceptor):
    """
    Interceptor that can process messages in the in-path and out-path. Just a
    wrapper for code to keep both complementing actions together.
    Note: There is NO guanantee that for one incoming message there is one
    outgoing message
    """
    def process(self, invocation):
        """
        @param invocation container object for parameters
        @retval invocation instance, may be modified
        """
        if invocation.path == Invocation.PATH_IN:
            return defer.maybeDeferred(self.before, invocation)
        elif invocation.path == Invocation.PATH_OUT:
            return defer.maybeDeferred(self.after, invocation)
        else:
            raise ConfigurationError("Illegal EnvelopeInterceptor path: %s" % invocation.path)

    def before(self, invocation):
        return invocation
    def after(self, invocation):
        return invocation

class PassThroughInterceptor(EnvelopeInterceptor):
    """
    Interceptor that drops messages.
    """
    def before(self, invocation):
        invocation.proceed()
        return invocation
    def after(self, invocation):
        invocation.proceed()
        return invocation

class DropInterceptor(EnvelopeInterceptor):
    """
    Interceptor that drops messages.
    """
    def before(self, invocation):
        invocation.drop()
        return invocation
    def after(self, invocation):
        invocation.drop()
        return invocation
