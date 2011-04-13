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
from telephus.cassandra.ttypes import KsDef, CfDef, ColumnDef, NotFoundException, IndexType, InvalidRequestException

from twisted.internet import defer
from twisted.internet import reactor
from ion.util.state_object import BasicStates


from ion.util import procutils as pu

from ion.core.process import process
from ion.core.data.cassandra import CassandraStore, CassandraIndexedStore, CassandraError
from ion.core.data.storage_configuration_utility import BLOB_CACHE, COMMIT_CACHE, PERSISTENT_ARCHIVE, STORAGE_PROVIDER
from ion.core.data import storage_configuration_utility
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)




class CassandraMixin(object):
    def __init__(self, username, password, storage_conf, connect_to_keyspace=True):
        """
        Get init args from the bootstrap
        """
        try:
            keyspace = storage_conf[PERSISTENT_ARCHIVE]["name"]
        except KeyError, ke:
            log.debug(ke)
            raise CassandraError('Error in storage_conf argument to initialize a bootstrap client!')

        log.info('CassandraBootStrap Args: Uname - %s, Password - %s, Keyspace - %s' % (username,password,keyspace))

        self._storage_conf = storage_conf

        log.debug('Configuring Cassandra Connection: %s' % str(storage_conf))
        host = storage_conf[STORAGE_PROVIDER]["host"]
        port = storage_conf[STORAGE_PROVIDER]["port"]
        self._keyspace = storage_conf[PERSISTENT_ARCHIVE]["name"]


        client_factory_kwargs = {'check_api_version':True}

        if connect_to_keyspace:
            client_factory_kwargs['keyspace'] = keyspace

        if username is not None and password is not None:
            authorization_dictionary = {"username":username, "password":password}
            client_factory_kwargs['credentials'] = authorization_dictionary

        self._manager = ManagedCassandraClientFactory(**client_factory_kwargs)

        log.info('CassandraBootStrap: Host - %s, Port - %s' % (host, port))

        TCPConnection.__init__(self,host, port, self._manager)
        self.client = CassandraClient(self._manager)
        log.info("Created Cassandra Client")

class CassandraIndexedStoreBootstrap(CassandraMixin, CassandraIndexedStore):
    
    def __init__(self, username, password, keyspace=None):

        storage_conf = storage_configuration_utility.get_cassandra_configuration(keyspace)
        CassandraMixin.__init__(self, username, password, storage_conf)

        #We must set self._query_attribute_names, because we don't call
        #CassandraIndexedStore.__init__

        self._query_attribute_names = None
        self._cache_name = COMMIT_CACHE
        log.info("leaving CassandraIndexedStoreBootstrap.__init__")



class CassandraStoreBootstrap(CassandraMixin, CassandraStore):

    def __init__(self, username, password, keyspace=None):

        storage_conf = storage_configuration_utility.get_cassandra_configuration(keyspace)
        CassandraMixin.__init__(self, username, password, storage_conf)
        self._cache_name = BLOB_CACHE


class CassandraSchemaError(Exception):
    """
    An exception class for the Cassandra Schema Initialization process
    """

class CassandraSchemaProvider(CassandraMixin, TCPConnection):

    def __init__(self, username, password, storage_conf, error_if_existing=True):

        CassandraMixin.__init__(self, username, password, storage_conf, connect_to_keyspace=False)
        self.error_if_existing = error_if_existing


    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):

        TCPConnection.on_activate(self)


        ks_cassandra = None
        try:
            ks_cassandra = yield self.client.describe_keyspace(self._keyspace)

            if self.error_if_existing:
                raise CassandraSchemaError('KeySpace already exists. Cassandra Schema Initialization expected to create a new Keyspace!')

        except NotFoundException, nfe:
            log.info(nfe)
            ks_cassandra = None


        ks_conf = build_telephus_ks(self._storage_conf[PERSISTENT_ARCHIVE])

        if ks_cassandra is None:
            log.debug('Creating Cassandra keyspace for CassandraInitialization: %s', self._keyspace)

            yield self.client.system_add_keyspace(ks_conf)
            yield self.client.set_keyspace(self._keyspace)

        else:
            yield self.client.set_keyspace(self._keyspace)

            if ks_cassandra == ks_conf:
                defer.returnValue(None)

            # Make sure the key space properties have not been changed in the configuration
            ks_properties = ['name', 'strategy_class', 'replication_factor']
            for prop in ks_properties:
                if getattr(ks_cassandra, prop) != getattr(ks_conf, prop):
                    raise CassandraSchemaError('Can not modify cassandra keyspace properties of an existing keyspace: property - "%s"' % prop)


            yield self._apply_cf_configuration(ks_conf, ks_cassandra)

        defer.returnValue(None)


    @defer.inlineCallbacks
    def _apply_cf_configuration(self, ks_conf, ks_cassandra):

        # CF properties which can not be modified - add others here?
        cf_props = ['column_type', 'comparator_type', 'subcomparator_type', 'comment', 'default_validation_class']

        needs_update = False

        # Make sure all the required column families are present
        cf_conf_names = {}
        for cf in ks_conf.cf_defs:
            cf_conf_names[cf.name] = cf

        for name in cf_conf_names:

            for cf_cass in ks_cassandra.cf_defs:

                if name == cf_cass.name:

                    cf_conf = cf_conf_names[name]
                    # add any columns that are needed! What does this *Really* do?
                    needs_update = self._apply_col_configuration(cf_conf, cf_cass)

                    for prop in cf_props:

                        if getattr(cf_cass, prop) != getattr(cf_conf, prop):
                            raise CassandraSchemaError('Can not modify cassandra column family properties of an existing column family: property - "%s"' % prop)


                    # Found the Column Family and applied columns...
                    if needs_update:
                        yield self.client.system_update_column_family(cf_cass)

                    break

            else:
                yield self.client.system_add_column_family(cf_conf_names[name])

    def _apply_col_configuration(self, cf_conf, cf_cass):
        """
        The CF_CASS is modified to include any columns that should be present
        """

        # Needs update?
        retval = False

        col_props = ['validation_class','index_type','index_name']

        col_conf_names={}
        for col in cf_conf.column_metadata:
            col_conf_names[col.name] = col

        for name in col_conf_names:

            for col_cass in cf_cass.column_metadata:

                if name == col_cass.name:

                    col_conf = col_conf_names[name]

                    for prop in col_props:
                        if getattr(col_conf, prop) != getattr(col_cass,prop):
                            raise CassandraSchemaError('Can not modify cassandra column properties of an existing column: property - "%s"' % prop)

                    break
            else:
                retval = True
                cf_cass.column_metadata.append(col_conf)

        return retval


    def on_deactivate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_deactivate: Lose Connection TCP')

    def on_terminate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_terminate: Lose Connection TCP')


def build_telephus_ks(storage_conf):
    """
    Be careful not to change the imported dictionary - copy everything!

    """
    # Create a copy of the
    ks_dict = storage_conf.copy()


    ks_kwargs = ks_dict.copy()
    ks_kwargs['cf_defs'] = []

    if ks_dict['cf_defs'] is None:
        ks_dict['cf_defs']=[]

    for cf_dict in ks_dict['cf_defs']:

        cf_kwargs = cf_dict.copy()
        cf_kwargs['column_metadata']=[]

        if cf_dict['column_metadata'] is None:
            cf_dict['column_metadata'] = []

        for col_dict in cf_dict['column_metadata']:
            # Create the telephus definition of each column from the dict
            col_def = ColumnDef(**col_dict)
            cf_kwargs['column_metadata'].append(col_def)

        # Create the telephus definition of each column family from the dict
        cf_def = CfDef(**cf_kwargs)
        ks_kwargs['cf_defs'].append(cf_def)


    return KsDef(**ks_kwargs)



class CassandraInitializationProcess(process.Process):


    def __init__(self, receiver=None, spawnargs=None, **kwargs):

        process.Process.__init__(self, receiver, spawnargs, **kwargs)

        uname = self.spawn_args.get('cassandra_username', None)
        pword = self.spawn_args.get('cassandra_password', None)
        keyspace = self.spawn_args.get('keyspace', None)
        error_if_existing = self.spawn_args.get('error_if_existing', True)

        storage_conf = storage_configuration_utility.get_cassandra_configuration(keyspace)

        self.cassandra_bootstrap = CassandraSchemaProvider(uname, pword, storage_conf, error_if_existing=error_if_existing)

        self.add_life_cycle_object(self.cassandra_bootstrap)



    def plc_activate(self, *args, **kwargs):

        reactor.callLater(1, self.terminate_when_active)

    def terminate_when_active(self):

        if self._get_state() is BasicStates.S_ACTIVE:
            self.terminate()
        else:
            log.warn('Process not yet active: waiting for termination!')
            reactor.callLater(1, terminate_when_active)


class CassandraTestHarnessClient(CassandraMixin, TCPConnection):

    def on_deactivate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_deactivate: Lose Connection TCP')

    def on_terminate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_terminate: Lose Connection TCP')
