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

def start(container, starttype, *args, **kwargs):
    log.info("ioncore starting")
    res = ('OK', 'pid', [])

    bootstrap.init_container()
    
    return defer.succeed(res)

def stop(container, state):
    log.info("ioncore stopping")
    return defer.succeed(None)
