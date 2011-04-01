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
import os

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


# --- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):

    
    ds_spawn_args = {PRELOAD_CFG:{ION_DATASETS_CFG:False}}
    # Check for command line argument to add some example data resources
    if ioninit.cont_args.get('register', None) == 'demodata':

        
        ### Rather than print the data_resources object - how do we add it to locals?
        ### I can't find the control object for the shell from here?
        print '================================================================='
        print 'Addind Data Resources:'
        print 'Data loaded from files may or may not be available depending on your configuration'
        print 'Import Dataset IDs from "ion.services.coi.datastore_bootstrap.ion_preload_config"'
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
