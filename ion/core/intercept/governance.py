#!/usr/bin/env python

"""
@file ion/core/intercept/policy.py
@author Michael Meisinger
@brief Governance tracking interceptor
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.intercept.interceptor import EnvelopeInterceptor, PassThroughInterceptor
import ion.util.procutils as pu


class GovernanceInterceptor(EnvelopeInterceptor):
    def before(self, invocation):
        return invocation

    def after(self, invocation):
        return invocation

del GovernanceInterceptor
GovernanceInterceptor = PassThroughInterceptor
