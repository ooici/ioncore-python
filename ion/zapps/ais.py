
from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor


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
         'spawnargs':{'datastore_service':'datastore'}},
        {'name':'app_integration',
         'module':'ion.integration.ais.app_integration_service',
         'class':'AppIntegrationService'},
        {'name':'identity_registry',
         'module':'ion.services.coi.identity_registry',
         'class':'IdentityRegistryService'}
        ]

    appsup_desc = ProcessDesc(name='app-supervisor-'+app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':resource_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])
    
    
    # Check for command line argument to add some example data resources
    if ioninit.cont_args.get('register',None) == 'dummydata':
        print '================================================================='
        print 'Dummy Dataset Resources added by the AIS Service:'
        #print data_resources
        print 'The dataset IDs will be available in your localsOkay after the shell starts!'
        print '================================================================='
    
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" +str(state) )
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()

    
