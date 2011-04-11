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
from telephus.cassandra.ttypes import KsDef, CfDef, ColumnDef, NotFoundException, IndexType

from ion.util import procutils as pu

from ion.core.data.cassandra import CassandraStore, CassandraIndexedStore
from ion.core.data.storage_configuration_utility import BLOB_CACHE, COMMIT_CACHE
from ion.core.data import storage_configuration_utility
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


'''
def _build_keyspace_def(name=None, strategy_class=None, strategy_options=None, replication_factor=None, cf_defs=None):


    ksdef = KsDef(name=None, strategy_class=None, strategy_options=None, replication_factor=None, cf_defs=None,)

    ksdef = KsDef(name=keyspace,
                  replication_factor=1,
                  strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                  )

    return ksdef
'''

def _build_column_family_def(keyspace, cf_name, col_defs=None):

    cols=[]
    if col_defs is not None:
        cols = col_defs

    return CfDef(keyspace = keyspace,
                 name = cf_name,
                 column_type='Standard',
                 comparator_type='org.apache.cassandra.db.marshal.BytesType',
                 column_metadata = cols)


def _build_colum_defs(names):
    columns = []
    for name in names:
        col = ColumnDef(
                name=name,
                validation_class='BytesType',
                index_type=IndexType.KEYS)
        columns.append(col)

    return columns


class CassandraBootStrap:
    def __init__(self, username, password, keyspace=None):
        """
        Get init args from the bootstrap
        """

        log.info('CassandraBootStrap Args: Uname - %s, Password - %s, Keyspace - %s' % (username,password,keyspace))

        storage_conf = storage_configuration_utility.get_storage_conf_dict(keyspace)

        self._storage_conf = storage_conf

        log.debug('Configuring Cassandra Connection: %s' % str(storage_conf))
        host = storage_conf["storage provider"]["host"]
        port = storage_conf["storage provider"]["port"]
        self._keyspace = storage_conf["persistent archive"]["name"]

        if username is None or password is None:
            self._manager = ManagedCassandraClientFactory(keyspace=self._keyspace,
                                                          check_api_version=True)
        else:
            authorization_dictionary = {"username":username, "password":password}
            self._manager = ManagedCassandraClientFactory(keyspace=self._keyspace,
                                                          credentials=authorization_dictionary,
                                                          check_api_version=True)


        log.info('CassandraBootStrap: Host - %s, Port - %s' % (host, port))

        TCPConnection.__init__(self,host, port, self._manager)
        self.client = CassandraClient(self._manager)
        log.info("Created Cassandra Client")

class CassandraIndexedStoreBootstrap(CassandraBootStrap, CassandraIndexedStore):
    
    def __init__(self, username, password, sysname=None, do_init=False):
        CassandraBootStrap.__init__(self, username, password, sysname)

        #We must set self._query_attribute_names, because we don't call
        #CassandraIndexedStore.__init__

        self._do_init = do_init
        self._query_attribute_names = None
        self._cache_name = COMMIT_CACHE
        log.info("leaving CassandraIndexedStoreBootstrap.__init__")


    def activate(self, *args, **kwargs):

        TCPConnection.activate(self)

        try:
            ks = yield self.client.describe_keyspace(self._keyspace)
        except NotFoundException:
            ks = None

        if ks is None:
            log.debug('Creating Cassandra keyspace for CassandraIndexedStoreBootstrap: %s', self._keyspace)

            cf_def = _build_column_family_def(self._keyspace, )



            ks = _build_keyspace_def(self._keyspace)

            ks.cf_defs.append()

            yield self.client.system_add_keyspace(ks)
            yield self.client.set_keyspace(keyspace)

        else:
            yield self.client.set_keyspace(keyspace)
            cfs = _build_column_family_defs(keyspace,
                                            self._launch_column_family,
                                            self._node_column_family)
            for cf in cfs:
                exists = False
                for existing_cf in ks.cf_defs:
                    if existing_cf.name == cf.name:
                        exists = True
                        break

                if not exists:
                    self._created_column_families = True
                    log.info("Creating missing Cassandra column family: " + cf.name)
                    yield self.client.system_add_column_family(cf)
        self._keyspace = keyspace




class CassandraStoreBootstrap(CassandraBootStrap, CassandraStore, do_init=False):

    def __init__(self, username, password, sysname=None):
        CassandraBootStrap.__init__(self, username, password, sysname)
        self._cache_name = BLOB_CACHE
        self._do_init = do_init