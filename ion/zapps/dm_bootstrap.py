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

from ion.services.coi.exchange.exchange_management import ExchangeManagementClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor

import ion.services.coi.exchange.exchange_resources as bp
from ion.services.coi.exchange.exchange_resources import ClientHelper

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
    log.debug('Starting helper...')
    emc_helper = ClientHelper(sup)
    log.info('Started, time for init.')

    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('DM bootstrap stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()
