#!/usr/bin/env python

"""
@file ion/zapps/dm_bootstrap.py
@author Paul Hubbard
@date 2/7/11
@brief DM bootstrap code, create science data space & point
"""

from twisted.internet import defer
import ion.util.ionlog
from ion.core import ioninit
from ion.services.coi.exchange.exchange_management import ExchangeManagementClient
from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

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
                    'module':'ion.services.coi.resource_registry_beta.resource_registry',
                    'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}
                },
                {
                    'name':'exchange_management',
                    'module':'ion.services.coi.exchange.exchange_management',
                    'class':'ExchangeManagementService',
                },
                {
                    'name':'pubsub',
                    'module':'ion.services.dm.distribution.pubsub_service',
                    'class' : 'PubSubService',

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
    log.info('Started, time for init.')

    # Create our exchange space
    log.debug('Creating exchange space...')

    # Humor - a swapmeet is a space to exchange things
    # This call will return an error if space already exists - ignore
    yield emc.create_exchangespace('swapmeet', 'DM exchange space')

    log.debug('Creating exchange point...')
    yield emc.create_exchangename('science_data', 'Science data XP', 'swapmeet')
    log.debug('DM bootstrapping completed.')
    
    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('DM bootstrap stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()
