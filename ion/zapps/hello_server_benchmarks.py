#!/usr/bin/env python

"""
@file ion/zapps/association.py
@author Matt Rodriguez
@brief simple app that tests the performance of the message stack
"""
import time
from twisted.internet import defer

from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc, Process
from ion.core.cc.shell import control

from ion.play.hello_service import HelloServiceClient

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
CONF = ioninit.config(__name__)

    
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    
    services = [
            {'name':'hello1','module':'ion.play.hello_service','class':'HelloService'},
    ]
    appsup_desc = ProcessDesc(name='app-supervisor-' + app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':services})
    #t1 = time.time()
    supid = yield appsup_desc.spawn()
    
    res = (supid.full, [appsup_desc])
    defer.returnValue(res)
    
@defer.inlineCallbacks
def stop(container, state):
    
    supdesc = state[0]
    yield supdesc.terminate()