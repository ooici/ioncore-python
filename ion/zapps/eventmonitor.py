#!/usr/bin/env python

"""
@file ion/zapps/attributestore.py
@author Dave Foster <dfoster@asascience.com>
@brief Attribute Store app - a dead simple service we can run for working samples
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    as_services =[{ 'name':'eventmonitor',
                     'module':'ion.services.dm.distribution.eventmonitor',
                     'class':'EventMonitorService'}]

    app_sup_desc = ProcessDesc(name="app-supervisor-" + app_definition.name,
                               module=app_supervisor.__name__,
                               spawnargs={'spawn-procs':as_services})

    supid = yield app_sup_desc.spawn()

    res = (supid.full, [app_sup_desc])
    log.info("Started EventMonitorService")
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("Stopping EventMonitorService")
    supdesc = state[0]
    yield supdesc.terminate()

