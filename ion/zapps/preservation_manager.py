#!/usr/bin/env python

"""
@file ion/zapps/preservation_manager.py
@author Matt Rodriguez
@date 2/14/11
@brief Preservation Manager application, an application to create Preservation Archives and caches on demand
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
from ion.core import ioninit
from ion.services.coi.exchange.exchange_management import ExchangeManagementClient
from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor
from ion.services.dm.preservation.cassandra_manager_agent import CassandraManagerClient

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
                {
                    'name':'exchange_management',
                    'module':'ion.services.coi.exchange.exchange_management',
                    'class':'ExchangeManagementService',
                },
                {
                    'name': 'cassandra_manager_agent',
                    'module': 'ion.services.dm.preservation.cassandra_manager_agent',
                    'class':'CassandraManagerAgent'
                },    
                
        ]

    log.debug('Starting application supervisor and required services...')
    app_supv_desc = ProcessDesc(name='DM app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':ems_proc})

    supv_id = yield app_supv_desc.spawn()

    # This line cribbed from DavidS's code - stinky hack
    sup = ioninit.container_instance.proc_manager.process_registry.kvs.get(supv_id, None)


    log.debug('Starting EMC...')
    emc = ExchangeManagementClient(proc=sup)
    log.debug('Starting CassandraManagerClient')
    cassandra_manager = CassandraManagerClient(proc=sup)
    log.info('Started, time for init.')

    # Create our exchange space
    log.debug('Creating exchange space...')

    # Humor - a swapmeet is a space to exchange things
    xs_name = 'swapmeet'

    # This call will return an error if space already exists - ignore
    yield emc.create_exchangespace(xs_name, 'DM exchange space')

    log.debug('Trying to invoke pubsub controller to create science data exchange point...')
    #yield psc.declare_topic_tree(xs_name, 'science_data')

    log.debug('DM bootstrapping completed.')
    
    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('DM bootstrap stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()
