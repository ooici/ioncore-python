#!/usr/bin/env python

"""
@file ion/res/config.py
@author David Stuebe
@author Tim LaRocque
@TODO
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor

#from ion.core.ioninit import ion_config
from ion.core import ioninit
from ion.core.cc.shell import control

from ion.services.coi.datastore_bootstrap.ion_preload_config import PRELOAD_CFG, ION_DATASETS_CFG, ION_DATASETS, CONTENT_ARGS_CFG, ID_CFG
#from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_PROFILE_DATASET_ID, SAMPLE_PROFILE_DATA_SOURCE_ID, \
#                                                                    SAMPLE_TRAJ_DATASET_ID, SAMPLE_TRAJ_DATA_SOURCE_ID, \
#                                                                    SAMPLE_STATION_DATASET_ID, SAMPLE_STATION_DATA_SOURCE_ID, \
#                                                                    PRELOAD_CFG, ION_DATASETS_CFG, ION_DATASETS, CONTENT_ARGS_CFG, ID_CFG
import os


# --- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):

    
    ds_spawn_args = {PRELOAD_CFG:{ION_DATASETS_CFG:False}}
    # Check for command line argument to add some example data resources
    if ioninit.cont_args.get('register', None) == 'demodata':

        # Determine which datasource resources 'should' be available
        data_resources = []
        for k, v in ION_DATASETS.items():
            add_me = True
            # If the Resource defines a filename in its content args,
            #   ensure that the file exists.  If not, don't add the corresponding resource...
            content_args = v.get(CONTENT_ARGS_CFG, None)
            if content_args:
                filename = content_args.get('filename', ':\0?')
                if filename is not ':\0?':
                    if filename is None or not os.path.exists(filename):
                        add_me = False
            
            if add_me:
                data_resources.append((str(k), str(v[ID_CFG])))
        
        data_resources.sort()
        
        ### Rather than print the data_resources object - how do we add it to locals?
        ### I can't find the control object for the shell from here?
        print '================================================================='
        print 'Added Data Resources:'
        print '{'
        for k, v in data_resources:
            print "\t'%s': '%s'," % (k, v)
            control.add_term_name(k, v)
        print '}'
        print 'The dataset IDs will be available in your localsOkay after the shell starts!'
        print '================================================================='

        ds_spawn_args = {PRELOAD_CFG:{ION_DATASETS_CFG:True}}


    resource_proc = [
        {'name':'ds1',
         'module':'ion.services.coi.datastore',
         'class':'DataStoreService',
         'spawnargs':ds_spawn_args
            },
        {'name':'resource_registry1',
         'module':'ion.services.coi.resource_registry_beta.resource_registry',
         'class':'ResourceRegistryService',
         'spawnargs':{'datastore_service':'datastore'}}
        ]

    appsup_desc = ProcessDesc(name='app-supervisor-' + app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':resource_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])






    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" + str(state))
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()
