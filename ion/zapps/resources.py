

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor

#from ion.core.ioninit import ion_config
from ion.core import ioninit
from ion.core.cc.shell import control

from ion.core.object import object_utils
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance


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

    appsup_desc = ProcessDesc(name='app-supervisor-'+app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':resource_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])
    
    
    # Check for command line argument to add some example data resources
    if ioninit.cont_args.get('register',None) == 'demodata':

        # Run script to create data objects
        data_resources = yield _bootstrap_objects(supid)
        
        ### Rather than print the data_resources object - how do we add it to locals?
        ### I can't find the control object for the shell from here?
        print '================================================================='
        print 'Added Data Resources:'
        print data_resources
        print 'The dataset IDs will be available in your localsOkay after the shell starts!'
        print '================================================================='
        for k,v in data_resources.items():
            control.add_term_name(k,v)
    
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" +str(state) )
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()
    
    
# Create CDM Type Objects
dataset_type = object_utils.create_type_identifier(object_id=10001, version=1)
group_type = object_utils.create_type_identifier(object_id=10020, version=1)

@defer.inlineCallbacks
def _bootstrap_objects(supid):
    
    print 'Sup:',supid
    
    # This is only for bootstrap - do no do this in operational code!
    sup = ioninit.container_instance.proc_manager.process_registry.kvs.get(supid, None)
    
    rc = ResourceClient(proc=sup)

    dataset = yield rc.create_instance(dataset_type, name='Test CDM Resource dataset', description='A test resource')
        
    group = dataset.CreateObject(group_type)
    
    group.name = 'junk data'
    
    dataset.root_group = group
    
    rc.put_instance(dataset, 'Testing put...')
        
    data_resources ={'dataset1':dataset.ResourceIdentity}
    
    defer.returnValue(data_resources)
