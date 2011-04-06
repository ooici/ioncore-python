#!/usr/bin/env python

"""
@file ion/zapps/datastore.py
@author Matt Rodriguez
@brief Datastore App -- application the runs the versioning service
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    as_services =[{ 'name':'datastore',
                     'module':'ion.services.coi.datastore',
                     'class':'DataStoreService'}]

    app_sup_desc = ProcessDesc(name="app-supervisor-" + app_definition.name,
                               module=app_supervisor.__name__,
                               spawnargs={'spawn-procs':as_services,
                                          'blobs': 'ion.core.data.cassandra_bootstrap.CassandraStoreBootstrap',
                                          'commits': 'ion.core.data.cassandra_bootstrap.CassandraIndexedStoreBootstrap',
                                          'username':'ooiuser',
                                          'password':'oceans11'
                                          })

    supid = yield app_sup_desc.spawn()

    res = (supid.full, [app_sup_desc])
    log.info("Started DataStoreService")
    
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("Stopping DataStoreService")
    supdesc = state[0]
    yield supdesc.terminate()

