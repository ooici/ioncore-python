#!/usr/bin/env python

"""
@file ion/core/pack/processapp.py
@author @author Michael Meisinger
@brief Standard app handler to start one process
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.exception import ConfigurationError
from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    if len(args) < 3:
        raise ConfigurationError("Arguments invalid for standard process app: args='%s', kwargs='%s'"% (args, kwargs))

    proc_name = args[0]
    proc_mod = args[1]
    proc_cls = args[2]

    app_proc = [{'name':proc_name, 'module':proc_mod, 'class':proc_cls, 'spawnargs':kwargs}]

    app_sup_desc = ProcessDesc(name="app-supervisor-" + app_definition.name,
                               module=app_supervisor.__name__,
                               spawnargs={'spawn-procs':app_proc})

    supid = yield app_sup_desc.spawn()

    res = (supid.full, [app_sup_desc])
    log.debug("Started %s" % proc_name)
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.debug("Stopping Process app")
    supdesc = state[0]
    yield supdesc.terminate()
