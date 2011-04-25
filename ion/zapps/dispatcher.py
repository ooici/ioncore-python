#!/usr/bin/env python

"""
@file ion/zapps/dispatcher.py
@author Tim LaRocque <tlarocque@asascience.com>
@brief EOI Dispatcher Service
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    services = [
        {
            'name':'pubsub_service1',
            'module':'ion.services.dm.distribution.pubsub_service',
            'class':'PubSubService'
        },{
            'name':'datastore1',
            'module':'ion.services.coi.datastore',
            'class':'DataStoreService',
            'spawnargs':{'servicename':'datastore'}
        },{
            'name':'resource_registry1',
            'module':'ion.services.coi.resource_registry.resource_registry',
            'class':'ResourceRegistryService',
            'spawnargs':{'datastore_service':'datastore'}
        },{
            'name':'exchange_management1',
            'module':'ion.services.coi.exchange.exchange_management',
            'class':'ExchangeManagementService',
        },{
           'name':'association_service1',
           'module':'ion.services.dm.inventory.association_service',
           'class':'AssociationService'
        }
    ]

    # Step 1: Spawn process dependencies
    app_sup_desc = ProcessDesc(name="supervisor-" + app_definition.name,
                               module=app_supervisor.__name__,
                               spawnargs={'spawn-procs':services})
    supid = yield app_sup_desc.spawn()



    res = (supid.full, [app_sup_desc])
    log.info('Started dispatcher service dependencies bundle')
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info('Stopping dispatcher service supervisor')
    supdesc = state[0]
    yield supdesc.terminate()

