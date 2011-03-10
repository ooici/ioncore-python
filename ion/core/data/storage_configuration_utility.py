#!/usr/bin/env python

"""
@file ion/core/data/storage_configuration_utility.py
@author David Stuebe
@TODO
"""
import os

from ion.core.exception import IonError
from ion.util.config import Config
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# Defined Terms:
BLOB_CACHE = 'blobs'
COMMIT_CACHE = 'commits'
CACHE_CONFIGURATION = 'cache configuration'
COMMIT_COLUMN_NAMES=[]

# This list must match what is in the storage.cfg file. I can't think of a better way to do this?
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


# Load the Config File!
storage_conf = Config('res/config/storage.cfg')

class StorageConfigurationError(IonError):
    """
    An exception to raise if the Storage configuration is incorrect or can not be read
    """

#TODO, do some checking to verify the Cassandra schema is configured correctly
"""
# Set some constants based on the config file:
cache_list = storage_conf.getValue(CACHE_CONFIGURATION,[])
for cache in cache_list:
    blob_cache = cache.get(BLOB_CACHE,None)
    if blob_cache:
        break
else:
    raise StorageConfigurationError('The storage configuration file does not have a cache for blobs!')


for cache in cache_list:
    commit_cache = cache.get(COMMIT_CACHE,None)
    if commit_cache:

        COMMIT_COLUMN_NAMES = commit_cache.get('indexed columns')

        assert REPOSITORY_KEY in COMMIT_COLUMN_NAMES, 'Repository key column name not found in config file!'
        assert BRANCH_NAME in COMMIT_COLUMN_NAMES, 'Branch Name column name not found in config file!'

        assert SUBJECT_KEY in COMMIT_COLUMN_NAMES, 'Subject key column name not found in config file!'
        assert SUBJECT_BRANCH in COMMIT_COLUMN_NAMES, 'Subject Branch column name not found in config file!'
        assert SUBJECT_COMMIT in COMMIT_COLUMN_NAMES, 'Subject Commit column name not found in config file!'

        assert PREDICATE_KEY in COMMIT_COLUMN_NAMES, 'Predicate Key column name not found in config file!'
        assert PREDICATE_BRANCH in COMMIT_COLUMN_NAMES, 'Predicate Branch column name not found in config file!'
        assert PREDICATE_COMMIT in COMMIT_COLUMN_NAMES, 'Predicate commit column name not found in config file!'

        assert KEYWORD in COMMIT_COLUMN_NAMES, 'Keyword column name not found in config file!'

        break
else:
    raise StorageConfigurationError('The storage configuration file does not have a cache for commits!')



log.info('BLOB_CACHE: %s' % BLOB_CACHE)
log.info('COMMIT_CACHE: %s' % COMMIT_CACHE)
log.info('CACHE_CONFIGURATION: %s' % CACHE_CONFIGURATION)

log.info('COMMIT_CACHE: %s' % COMMIT_CACHE)
log.info('COMMIT_COLUMN_NAMES: %s' % COMMIT_COLUMN_NAMES)

"""

