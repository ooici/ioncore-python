#!/usr/bin/env python
"""
@file ion/core/data/cassandra.py
@author Paul Hubbard
@author Michael Meisinger
@author Paul Hubbard
@author Dorian Raymer
@author Matt Rodriguez
@author David Stuebe
@brief Implementation of ion.data.store.IStore using Telephus to interface a
        Cassandra datastore backend
@note Test cases for the cassandra backend are now in ion.data.test.test_store
"""

from twisted.internet import defer

from zope.interface import implements

from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory
from telephus.cassandra.ttypes import NotFoundException, KsDef, CfDef
from telephus.cassandra.ttypes import ColumnDef, IndexExpression, IndexOperator

from ion.core.data import store
from ion.core.data.store import Query

from ion.core.data.store import IndexStoreError

from ion.util.tcp_connections import TCPConnection

from ion.core.data.cassandra import CassandraStore, CassandraIndexedStore

# Get a bunch of Global variable to use in bootstrapping the store
from ion.core.data.storage_configuration_utility import BLOB_CACHE, COMMIT_CACHE
from ion.core.data.storage_configuration_utility import COMMIT_COLUMN_NAMES
from ion.core.data.storage_configuration_utility import REPOSITORY_KEY, BRANCH_NAME

from ion.core.data.storage_configuration_utility import SUBJECT_KEY, SUBJECT_BRANCH, SUBJECT_COMMIT
from ion.core.data.storage_configuration_utility import PREDICATE_KEY, PREDICATE_BRANCH, PREDICATE_COMMIT
from ion.core.data.storage_configuration_utility import OBJECT_KEY, OBJECT_BRANCH, OBJECT_COMMIT

from ion.core.data.storage_configuration_utility import KEYWORD


import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)






class CassnadraIndexedStoreBootstrap(CassandraIndexedStore):

    def __init__(self):
        """
        Get init args from the bootstrap
        """

class CassnadraStoreBootstrap(CassandraStore):

    def __init__(self):
        """
        Get init args from the bootstrap
        """

