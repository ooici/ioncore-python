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

def start(container, starttype, *args, **kwargs):
    log.info("ioncore starting")
    res = ('OK', 'pid', [])
    return defer.succeed(res)

def stop(container, state):
    log.info("ioncore stopping")
    return defer.succeed(None)
