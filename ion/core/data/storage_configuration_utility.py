#!/usr/bin/env python

"""
@file ion/core/data/storage_configuration_utility.py
@author David Stuebe
@TODO
"""

from ion.core import ioninit
from telephus.cassandra.ttypes import IndexType

# get configuration
CONF = ioninit.config(__name__)

# Defined Terms:

### PRESERVATION SERVICE TERMS
STORAGE_PROVIDER = 'storage provider'
PERSISTENT_ARCHIVE = 'persistent archive'

DEFAULT_KEYSPACE_NAME ='DEFAULT NAME - DO NOT USE'

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
           'validation_class':'org.apache.cassandra.db.marshal.BytesType',
           'index_type':None,
           'index_name':None}


base_cf_def ={
    'keyspace' : DEFAULT_KEYSPACE_NAME,
    'name' : None,
    'column_type' : 'Standard',
    'comparator_type' : 'org.apache.cassandra.db.marshal.BytesType',
    'subcomparator_type' : None,
    'comment' : None,
    'column_metadata' : None,
    'default_validation_class':'org.apache.cassandra.db.marshal.BytesType',
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
    col_def['index_type']=IndexType.KEYS
    commit_cols.append(col_def)


commit_cf = base_cf_def.copy()
commit_cf['name']=COMMIT_CACHE
commit_cf['column_metadata'] = commit_cols

blob_cf = base_cf_def.copy()
blob_cf['name']=BLOB_CACHE
# No columns to declare for indexing

### Storage Keyspace Name is provided by the sysname!!!
#ion_ks = base_ks_def.copy()
#ion_ks['cf_defs'] = [blob_cf, commit_cf]

###
# CREATE A SINGLE EXPORTABLE DATA STRUCTURE

### This is the cassandra cluster details - do not put credentials in a config file!
storage_provider = {'host':'localhost', # ec2-184-72-14-57.us-west-1.compute.amazonaws.com',
                    'port':9160,
                    }

class StorageConfigurationError(Exception):
    '''
    An exception thrown due to invalid configuration of the cassandra storage
    '''


def get_cassandra_configuration(sysname=None):
    """
    Create a copy of all the components and over ride settings based on the configuration entry for this module

    'ion.core.data.storage_configuration_utility':{
    'storage provider':{'host':'ec2-184-72-14-57.us-west-1.compute.amazonaws.com','port':9160},
    'persistent archive':{'strategy_class':'org.apache.cassandra.locator.SimpleStrategy',
                            'replication_factor':1,}
    },

    """
    my_blob_cf = blob_cf.copy()
    my_commit_cf = commit_cf.copy()

    ion_ks = base_ks_def.copy()

    # Create the return value object
    confdict = {
        STORAGE_PROVIDER:storage_provider.copy(),
        PERSISTENT_ARCHIVE:ion_ks,
        }


    # set the storage provider information - host and port
    conf_provider = CONF.getValue(STORAGE_PROVIDER, {})
    for k, v in conf_provider.iteritems():
        if k not in storage_provider:
            raise StorageConfigurationError('Invalid storage provider configuration: key - "%s", value - "%s"' % (k,v))
        else:
            confdict[STORAGE_PROVIDER][k]=v

    # Set the values in the Key Space configuration
    conf_pa = CONF.getValue(PERSISTENT_ARCHIVE, {})
    if 'name' in conf_pa:
        raise StorageConfigurationError('Invalid Configuration for Persistent Archive: the name of the keyspace can not be specified in the CONF file. The sysname is always used.')

    for k, v in conf_pa.iteritems():
        if k not in base_ks_def:
            raise StorageConfigurationError('Invalid keyspace configuration: key - "%s", value - "%s"' % (k,v))
        else:
            ion_ks[k]=v

    # The blob cache and the commit cache are not configurable in this object!
    if ion_ks['cf_defs'] is None:
        ion_ks['cf_defs'] =[]

    ion_ks['cf_defs'].extend( [my_blob_cf, my_commit_cf])

    # update the sysname
    sysname = sysname or ioninit.sys_name
    #Keyspaces cannot have hyphens in their name. Replace hyphens with underscores. 
    sysname = sysname.replace("-", "_")
    if sysname is None:
        raise StorageConfigurationError("storage_configuration_utility.py: no ioninit.sysname or sysname provided to get_cassandra_configuration")

    # Set the keyspace name to the sysname!
    ion_ks['name'] = sysname
    my_blob_cf['keyspace'] = sysname
    my_commit_cf['keyspace'] = sysname

    return confdict

