#!/usr/bin/env python

"""
@file ion/zapps/resource_registry.py
@author Dave Foster <dfoster@asascience.com>
@brief Resource Registry application
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    as_services =[{ 'name':'resource_registry',
                     'module':'ion.services.coi.resource_registry_beta.resource_registry',
                     'class':'ResourceRegistryService'}]

    app_sup_desc = ProcessDesc(name="app-supervisor-" + app_definition.name,
                               module=app_supervisor.__name__,
                               spawnargs={'spawn-procs':as_services})

    supid = yield app_sup_desc.spawn()

    res = (supid.full, [app_sup_desc])
    log.info("Started ResourceRegistryService")
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("Stopping ResourceRegistryService")
    supdesc = state[0]
    yield supdesc.terminate()
