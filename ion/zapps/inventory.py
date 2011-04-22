#!/usr/bin/env python

"""
@file ion/zapps/dm_bootstrap.py
@author Paul Hubbard
@date 2/7/11
@brief DM bootstrap code, create science data space & point
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
from ion.core import ioninit
from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor
from ion.services.dm.inventory.inventory_service import CassandraInventoryClient


# Global(s)
log = ion.util.ionlog.getLogger(__name__)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('DM bootstrap container starting, startup type "%s"' % starttype)

    # Required services.
    ems_proc = [
                {
                    'name':'ds1',
                    'module':'ion.services.coi.datastore',
                    'class':'DataStoreService',
                    'spawnargs':{'servicename':'datastore'}
                },
                {
                    'name':'resource_registry1',
                    'module':'ion.services.coi.resource_registry.resource_registry',
                    'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}
                },
                {'name': 'inventory',
             'module': 'ion.services.dm.inventory.inventory_service',
             'class':'CassandraInventoryService'}  
        ]

    log.debug('Starting application supervisor and required services...')
    app_supv_desc = ProcessDesc(name='DM app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':ems_proc})

    supv_id = yield app_supv_desc.spawn()

    # This line cribbed from DavidS's code - stinky hack
    sup = ioninit.container_instance.proc_manager.process_registry.kvs.get(supv_id, None)


    cassandra_inventory_client = CassandraInventoryClient(proc=sup)
    log.debug('Starting pubsub client')
    #psc = PubSubClient(proc=sup)
    
    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('DM bootstrap stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()
