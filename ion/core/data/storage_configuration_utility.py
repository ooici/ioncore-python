#!/usr/bin/env python

"""
@file ion/core/data/storage_configuration_utility.py
@author David Stuebe
@TODO
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# Defined Terms:

### PRESERVATION SERVICE TERMS
STORAGE_PROVIDER = 'storage provider'
PERSISTENT_ARCHIVE = 'persistent archive'
CACHE_CONFIGURATION = 'cache configuration'

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

KEYWORD = 'keyword'

COMMIT_INDEXED_COLUMNS=[REPOSITORY_KEY, BRANCH_NAME, SUBJECT_KEY, SUBJECT_BRANCH, SUBJECT_COMMIT, PREDICATE_KEY,
                     PREDICATE_BRANCH, PREDICATE_COMMIT, OBJECT_KEY, OBJECT_BRANCH, OBJECT_COMMIT, KEYWORD]

# Common Columns:
VALUE = 'value'

###

# CREATE A SINGLE EXPORTABLE DATA STRUCTURE

STORAGE_CONF_DICTIONARY = {

### This is the cassandra cluster details - do not put credentials in a config file!
STORAGE_PROVIDER:{'host':'ec2-184-72-14-57.us-west-1.compute.amazonaws.com',
                    'port':9160
                    },
### Storage Keyspace is provided by the sysname!!!
PERSISTENT_ARCHIVE:{'name':'sysname',
                      'replication_factor':2,
                      'placement_strategy':'NetworkTopologyStrategy',
                      },
### Column Families
CACHE_CONFIGURATION: {BLOB_CACHE:{
    					'indexed columns':BLOB_INDEXED_COLUMNS},

     					COMMIT_CACHE:{
        					'indexed columns':COMMIT_INDEXED_COLUMNS}
        				},
}

### LOG SOME DEBUG
# @TODO Adde some more debug here!
log.info('BLOB CACHE NAME: %s' % BLOB_CACHE)
log.info('BLOB INDEXED COLUMNS: %s' % BLOB_INDEXED_COLUMNS)

log.info('COMMIT CACHE NAME: %s' % COMMIT_CACHE)
log.info('COMMIT INDEXED COLUMNS: %s' % COMMIT_INDEXED_COLUMNS)
