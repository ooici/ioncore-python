#!/usr/bin/env python

"""
@file ion/zapps/example.py
@author Michael Meisinger
@brief Example Capability Container application
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

def start(container, starttype, *args, **kwargs):
    log.info("App Example starting, starttype %s. args=%s, kwargs=%s" % (starttype,args,kwargs))
    res = ('pid', [])
    return defer.succeed(res)

def stop(container, state):
    log.info("App Example stopping")
    return defer.succeed(None)
