#!/usr/bin/env python

"""
@file ion/core/intercept/policy.py
@author Michael Meisinger
@brief Policy checking interceptor
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.intercept.interceptor import EnvelopeInterceptor, PassThroughInterceptor
import ion.util.procutils as pu


class PolicyInterceptor(EnvelopeInterceptor):
    def before(self, invocation):
        return invocation

    def after(self, invocation):
        return invocation

del PolicyInterceptor
PolicyInterceptor = PassThroughInterceptor
