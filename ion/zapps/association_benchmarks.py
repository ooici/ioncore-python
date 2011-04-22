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
import time

from ion.services.coi.datastore_bootstrap import dataset_bootstrap
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG
from ion.services.coi.datastore_bootstrap.ion_preload_config import ID_CFG, TYPE_CFG
from ion.services.coi.datastore_bootstrap.ion_preload_config import NAME_CFG,DESCRIPTION_CFG 
from ion.services.coi.datastore_bootstrap.ion_preload_config import CONTENT_CFG, CONTENT_ARGS_CFG
from ion.services.coi.datastore_bootstrap.ion_preload_config import ANONYMOUS_USER_ID, OWNED_BY_ID, HAS_LIFE_CYCLE_STATE_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_DATASETS
from ion.services.coi.datastore_bootstrap.ion_preload_config import IDENTITY_RESOURCE_TYPE_ID , TYPE_OF_ID, SAMPLE_PROFILE_DATASET_ID

from ion.services.dm.inventory.association_service import AssociationServiceClient

from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE, SUBJECT_PREDICATE_QUERY_TYPE

from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.core.object import object_utils

from ion.core.cc.shell import control
#-- CC Application interface

# Functions required
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
LCS_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=26, version=1)

@defer.inlineCallbacks
def find_by_owner():   
    association_client = AssociationServiceClient()
    
    request = yield association_client.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
    
    pair = request.pairs.add()
    
    # Set the predicate search term
    pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
    pref.key = OWNED_BY_ID

    pair.predicate = pref

    # Set the Object search term

    type_ref = request.CreateObject(IDREF_TYPE)
    type_ref.key = ANONYMOUS_USER_ID

    pair.object = type_ref
    #Uncomment for FAIL
    result = yield association_client.get_subjects(request)
    
    #print len(result)
    
    key_list = []
    for idref in result.idrefs:
        key_list.append(idref.key)
      
    defer.returnValue(key_list)
        
@defer.inlineCallbacks
def find_by_lcs():
    association_client = AssociationServiceClient()
    request = yield association_client.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)


    pair = request.pairs.add()

    # Set the predicate search term
    pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
    pref.key = TYPE_OF_ID

    pair.predicate = pref

    # Set the Object search term

    type_ref = request.CreateObject(IDREF_TYPE)
    type_ref.key = IDENTITY_RESOURCE_TYPE_ID

    pair.object = type_ref
    # Add a life cycle state request
    pair = request.pairs.add()

    # Set the predicate search term
    pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
    pref.key = HAS_LIFE_CYCLE_STATE_ID

    pair.predicate = pref


    # Set the Object search term
    state_ref = request.CreateObject(LCS_REFERENCE_TYPE)
    state_ref.lcs = state_ref.LifeCycleState.ACTIVE
    pair.object = state_ref

    result = yield association_client.get_subjects(request)
    key_list = []
    for idref in result.idrefs:
        key_list.append(idref.key)
        
    defer.returnValue(key_list)    

@defer.inlineCallbacks
def find_by_predicate():
    association_client = AssociationServiceClient()
    request = yield association_client.proc.message_client.create_instance(SUBJECT_PREDICATE_QUERY_TYPE)

    pair = request.pairs.add()

    # Set the predicate search term
    pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
    pref.key = OWNED_BY_ID

    pair.predicate = pref


    # Set the Subbject search term

    type_ref = request.CreateObject(IDREF_TYPE)
    type_ref.key = SAMPLE_PROFILE_DATASET_ID

    pair.subject = type_ref

    # make the request
    result = yield association_client.get_objects(request)


    key_list = []
    for idref in result.idrefs:
        key_list.append(idref.key)

    defer.returnValue(key_list)
         
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
        
    
    DATASET_TYPE = object_utils.create_type_identifier(object_id=10001, version=1)
    station_dataset_name = 'sample_station_dataset'
    stn_dataset_loc = CONF.getValue(station_dataset_name, None)
    station_dataset_template =  {ID_CFG:'',
                                 TYPE_CFG:DATASET_TYPE,
                                 NAME_CFG:station_dataset_name,
                                 DESCRIPTION_CFG:'An example of a station dataset',
                                 CONTENT_CFG:dataset_bootstrap.bootstrap_profile_dataset,
                                 CONTENT_ARGS_CFG:{'filename':stn_dataset_loc},
                                 }
    log.info("stn_dataset_loc: %s " % (stn_dataset_loc))
    def make_datasets(num):
        for i in range(num):
            ds = dict.copy(station_dataset_template)
            ds[ID_CFG] = str(i)
            ds[NAME_CFG] = "".join((station_dataset_name, str(i)))
            yield ds[NAME_CFG],ds
     
    num_datasets = 100
    datasets = dict([ds for ds in make_datasets(num_datasets)])
    ION_DATASETS.update(datasets)
    
    appsup_desc = ProcessDesc(name='app-supervisor-' + app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':services})
    t1 = time.time()
    supid = yield appsup_desc.spawn()
    t2 = time.time()
    diff = t2 - t1
    #log.critical("spawn time %f"% ( diff))
    print "spawn time %f" % (diff,)
    res = (supid.full, [appsup_desc])
    
    control.add_term_name('find_by_owner',find_by_owner)
    control.add_term_name('find_by_lcs',find_by_lcs)
    control.add_term_name('find_by_predicate',find_by_predicate)
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" + str(state))
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()
