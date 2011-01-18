

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor

from ion.core.ioninit import ion_config


# --- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    resource_proc = [
        {'name':'ds1',
         'module':'ion.services.coi.datastore',
         'class':'DataStoreService',
         'spawnargs':{'servicename':'datastore'}
            },
        {'name':'resource_registry1',
         'module':'ion.services.coi.resource_registry_beta.resource_registry',
         'class':'ResourceRegistryService',
         'spawnargs':{'datastore_service':'datastore'}}
        ]
    
    print '/////////////////////////////////////'
    print args
    print kwargs
    print '/////////////////////////////////////'


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
    