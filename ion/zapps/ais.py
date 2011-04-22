
from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.object import object_utils
from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS, COMMIT_CACHE
from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG




# --- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    resource_proc = [
        {'name':'app_integration',
         'module':'ion.integration.ais.app_integration_service',
         'class':'AppIntegrationService'},
        {'name':'index_store_service',
         'module':'ion.core.data.index_store_service',
         'class':'IndexStoreService',
                'spawnargs':{'indices':COMMIT_INDEXED_COLUMNS}},
        {'name':'ds1',
         'module':'ion.services.coi.datastore',
         'class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True},
                          COMMIT_CACHE:'ion.core.data.store.IndexStore'}},
        {'name':'association_service', 'module':'ion.services.dm.inventory.association_service', 'class':'AssociationService'},
        {'name':'resource_registry1',
         'module':'ion.services.coi.resource_registry.resource_registry',
         'class':'ResourceRegistryService',
         'spawnargs':{'datastore_service':'datastore'}},
        {'name':'identity_registry',
         'module':'ion.services.coi.identity_registry',
         'class':'IdentityRegistryService'}
        ]

    appsup_desc = ProcessDesc(name='app-supervisor-'+app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':resource_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])
    
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" +str(state) )
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()

    
