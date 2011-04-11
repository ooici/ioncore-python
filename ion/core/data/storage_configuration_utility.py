#!/usr/bin/env python

"""
@file ion/core/data/storage_configuration_utility.py
@author David Stuebe
@TODO
"""

from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# get configuration
CONF = ioninit.config(__name__)

# Defined Terms:

### PRESERVATION SERVICE TERMS
STORAGE_PROVIDER = 'storage provider'
PERSISTENT_ARCHIVE = 'persistent archive'
CACHE_CONFIGURATION = 'cache configuration'

DEFAULT_KEYSPACE_NAME ='DEFAULT NAME - DO NOT USE'

ION_KEYSPACE = 'ion keyspace'

### BLOB CACHE SETUP
BLOB_CACHE = 'blobs'
BLOB_INDEXED_COLUMNS=[]


### COMMIT CACHE SETUP
COMMIT_CACHE = 'commits'

REPOSITORY_KEY = 'repository_key'
BRANCH_NAME = 'branch_name'

SUBJECT_KEY = 'subject_key'
SUBJECT_BRANCH = 'subject_branch'
SUBJECT_COMMIT = 'subject_commit'

PREDICATE_KEY = 'predicate_key'
PREDICATE_BRANCH = 'predicate_branch'
PREDICATE_COMMIT = 'predicate_commit'

OBJECT_KEY = 'object_key'
OBJECT_BRANCH = 'object_branch'
OBJECT_COMMIT = 'object_commit'

RESOURCE_LIFE_CYCLE_STATE = 'resource_life_cycle_state'
RESOURCE_OBJECT_TYPE = 'resource_object_type'

KEYWORD = 'keyword'

COMMIT_INDEXED_COLUMNS=[REPOSITORY_KEY, BRANCH_NAME, SUBJECT_KEY, SUBJECT_BRANCH, SUBJECT_COMMIT, PREDICATE_KEY,
                     PREDICATE_BRANCH, PREDICATE_COMMIT, OBJECT_KEY, OBJECT_BRANCH, OBJECT_COMMIT, KEYWORD, RESOURCE_LIFE_CYCLE_STATE, RESOURCE_OBJECT_TYPE]


# Common Columns:
VALUE = 'value'

### Build up datastructures

base_col_def = {'name':None,
           'validation_class':'BytesType',
           'index_type':None,
           'index_name':None}


base_cf_def ={
    'keyspace' : DEFAULT_KEYSPACE_NAME,
    'name' : None,
    'column_type' : 'Standard',
    'comparator_type' : 'BytesType',
    'subcomparator_type' : None,
    'comment' : None,
    'column_metadata' : None,
    'default_validation_class':'BytesType',
}

base_ks_def = {
    'name':DEFAULT_KEYSPACE_NAME,
    'strategy_class':'org.apache.cassandra.locator.SimpleStrategy',
    #'strategy_options':'',
    'replication_factor':1,
    'cf_defs':None,
}


commit_cols = []
for col_name in COMMIT_INDEXED_COLUMNS:
    col_def = base_col_def.copy()
    col_def['name']=col_name
    commit_cols.append(col_def)


commit_cf = base_cf_def.copy()
commit_cf['name']=COMMIT_CACHE
commit_cf['column_metadata'] = commit_cols

blob_cf = base_cf_def.copy()
blob_cf['name']=BLOB_CACHE


ion_ks = base_ks_def.copy()
ion_ks['cf_defs'] = [blob_cf, commit_cf]

###
# CREATE A SINGLE EXPORTABLE DATA STRUCTURE

STORAGE_CONF_DICTIONARY = {

### This is the cassandra cluster details - do not put credentials in a config file!
STORAGE_PROVIDER:{'host':'localhost', # ec2-184-72-14-57.us-west-1.compute.amazonaws.com',
                    'port':9160
                    },
### Storage Keyspace is provided by the sysname!!!
PERSISTENT_ARCHIVE:{ION_KEYSPACE:ion_ks},

}

def get_storage_conf_dict(sysname=None):
    # shallow copy conf dict
    confdict = STORAGE_CONF_DICTIONARY.copy()

    # update configuration from ion.config file


    if 'name' in CONF.getValue(PERSISTENT_ARCHIVE, {}):
        raise KeyError('The keyspace name can not be set from the CONF file.')

    confdict[STORAGE_PROVIDER].update(CONF.getValue(STORAGE_PROVIDER, {}))
    confdict[PERSISTENT_ARCHIVE].update(CONF.getValue(PERSISTENT_ARCHIVE, {}))

    # Do not allow override of the Cache Configuration defined here!
    #confdict[CACHE_CONFIGURATION].update(CONF.getValue(CACHE_CONFIGURATION, {}))

    # update the sysname
    sysname = sysname or ioninit.sys_name
    assert sysname, "storage_configuration_utility.py: no ioninit.sysname or sysname provided on command line"

    confdict[PERSISTENT_ARCHIVE][ION_KEYSPACE] = sysname


    
    v['keyspace'] = sysname


    return confdict

### LOG SOME DEBUG
# @TODO Adde some more debug here!
log.info('BLOB CACHE NAME: %s' % BLOB_CACHE)
log.info('BLOB INDEXED COLUMNS: %s' % BLOB_INDEXED_COLUMNS)

log.info('COMMIT CACHE NAME: %s' % COMMIT_CACHE)
log.info('COMMIT INDEXED COLUMNS: %s' % COMMIT_INDEXED_COLUMNS)
