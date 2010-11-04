#!/usr/bin/env python

"""
@file ion/core/pack/ioncore_app.py
@author Michael Meisinger
@brief Application module for ioncore
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import bootstrap

@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    yield bootstrap.init_ioncore()

    res = (None,[])
    defer.returnValue(res)

def stop(container, state):
    return defer.succeed(None)
