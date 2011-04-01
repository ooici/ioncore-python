#!/usr/bin/env python
"""
@file ion/core/data/cassandra_bootstrap.py
@author David Stuebe
@author Matt Rodriguez

This class creates a connection to a Cassandra cluster without using ION Resources. 
This is useful when bootstrapping the system, because the datastore and the resource registry
services are not running yet. 
"""

from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory
from ion.util.tcp_connections import TCPConnection

from ion.core.data.cassandra import CassandraStore, CassandraIndexedStore
from ion.core.data.storage_configuration_utility import BLOB_CACHE, COMMIT_CACHE
from ion.core.data import storage_configuration_utility
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


class CassandraBootStrap:
    def __init__(self, username, password):
        """
        Get init args from the bootstrap
        """
        storage_conf = storage_configuration_utility.STORAGE_CONF_DICTIONARY
        #storage_conf = Config("res/config/storage.cfg")
        host = storage_conf["storage provider"]["host"]
        port = storage_conf["storage provider"]["port"]
        self._keyspace = storage_conf["persistent archive"]["name"]
        authorization_dictionary = {"username":username, "password":password}    
        self._manager = ManagedCassandraClientFactory(keyspace=self._keyspace, credentials=authorization_dictionary)
        TCPConnection.__init__(self,host, port, self._manager)
        self.client = CassandraClient(self._manager)


class CassandraIndexedStoreBootstrap(CassandraBootStrap, CassandraIndexedStore):
    
    def __init__(self, username, password):
        CassandraBootStrap.__init__(self, username, password)
        self._cache_name = COMMIT_CACHE

class CassandraStoreBootstrap(CassandraBootStrap, CassandraStore):

    def __init__(self, username, password):
        CassandraBootStrap.__init__(self, username, password)
        self._cache_name = BLOB_CACHE