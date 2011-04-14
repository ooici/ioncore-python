#!/usr/bin/env python

"""
@file ion/zapps/association.py
@author David Stuebe
@author Tim LaRocque
@author Matt Rodriguez
@brief An association application that uses a datastore service with a Cassandra backend.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor

#from ion.core.ioninit import ion_config
from ion.core import ioninit
import time

from ion.services.coi.datastore_bootstrap import dataset_bootstrap
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG
from ion.services.coi.datastore_bootstrap.ion_preload_config import ID_CFG, TYPE_CFG
from ion.services.coi.datastore_bootstrap.ion_preload_config import NAME_CFG,DESCRIPTION_CFG 
from ion.services.coi.datastore_bootstrap.ion_preload_config import CONTENT_CFG, CONTENT_ARGS_CFG

from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_DATASETS

from ion.services.dm.inventory.association_service import AssociationServiceClient

from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.core.object import object_utils
#-- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):

    
    # Get the username for cassandra from the command line. If not present get username from the kwargs for the app.
    uname = ioninit.cont_args.get('username', kwargs.get('username', None))
    pword = ioninit.cont_args.get('password', kwargs.get('password', None))


    if pword is None:
        print_pword = None
    else:
        print_pword = '****'
    log.info('Starting Association Service Instance: username: %s, password: %s' % (uname, print_pword))


    spawnargs = {'username':uname,
                  'password':pword,
                  }

    services = [
        {'name':'ds1',
         'module':'ion.services.coi.datastore',
         'class':'DataStoreService',
         'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True}}
        },
        {'name':'workbench_test1',
         'module':'ion.core.object.test.test_workbench',
         'class':'WorkBenchProcess',
         'spawnargs':{'proc-name':'wb1'}
        },

        {'name':'association_service',
         'module':'ion.services.dm.inventory.association_service',
         'class':'AssociationService'
          }
    ]
    association_proc = [
        {'name':'association_service',
         'module':'ion.services.dm.inventory.association_service',
         'class':'AssociationService',
         'spawnargs':spawnargs
        }
        ]
    TESTING_SIGNIFIER = '3319A67F'
    DATASET_TYPE = object_utils.create_type_identifier(object_id=10001, version=1)
    station_dataset_name = 'sample_station_dataset'
    stn_dataset_loc = CONF.getValue(station_dataset_name, None)
    station_dataset_template =  {ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F4',
                                 TYPE_CFG:DATASET_TYPE,
                                 NAME_CFG:station_dataset_name,
                                 DESCRIPTION_CFG:'An example of a station dataset',
                                 CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                                 CONTENT_ARGS_CFG:{'filename':stn_dataset_loc},
                                 }
    log.info("stn_dataset_loc: %s " % (stn_dataset_loc))
    def make_datasets(num):
        ds = station_dataset_template
        id_cfg = ds[ID_CFG]
        for i in range(num):
            ds[ID_CFG] = "-".join((id_cfg,str(i)))
            ds[NAME_CFG] = "".join((station_dataset_name, str(i)))
            yield ds[NAME_CFG],ds
     
    num_datasets = 10       
    datasets = dict([ds for ds in make_datasets(num_datasets)])
    ION_DATASETS.update(datasets)
    
    appsup_desc = ProcessDesc(name='app-supervisor-' + app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':services})
    t1 = time.time()
    supid = yield appsup_desc.spawn()
    t2 = time.time()
    diff = t2 - t1
    log.critical("spawn time %f"% ( diff))
    res = (supid.full, [appsup_desc])
    sup = ioninit.container_instance.proc_manager.process_registry.kvs.get(supid, None)
    association_client = AssociationServiceClient(sup)




    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" + str(state))
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()
