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
import os
import time

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

from ion.util.timeout import timeout

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
CONF = ioninit.config(__name__)


#import binascii


class SizeStats(object):
    """
    It isn't pretty but it works
    """

    def __init__(self):
        self.sum_time = 0.0
        self.sum_tpb = 0.0
        self.max_time = 0.0
        self.t_count    = 0
        self.sum_size = 0
        self.max_size = 0
        self.s_count    = 0

    def add_stats(self,tic,toc,size=0):
        etime = toc - tic
        self.sum_time += etime
        self.max_time = max(self.max_time, etime)

        self.sum_size += size
        self.max_size = max(self.max_size, size)

        self.t_count += 1

        if size is not 0:
            self.sum_tpb  += etime/(size/1000.)

            self.s_count +=1


    def put_stats(self):
        return (self.t_count,
                self.sum_time/self.t_count,
                self.sum_tpb/self.s_count,
                self.max_time,
                self.sum_size/(self.s_count*1000.),
                self.max_size/1000.)

    def get_stats(self):
        return (self.t_count,
                self.sum_time/self.t_count,
                self.sum_tpb/self.s_count,
                self.max_time,
                self.sum_size/(self.s_count*1000.),
                self.max_size/1000.,
                self.t_count - self.s_count)

    def simple_stats(self):
        return (self.t_count,
                self.sum_time/self.t_count,
                self.max_time)


class QueryStats(object):
    """
    It is better than the size stats...
    """

    def __init__(self):
        self.sum_time = [0.0] * 5
        self.max_time = [0.0] * 5
        self.count    = [0]   * 5
        self.sum_results = [0]   * 5
        self.t_count  = 0


    def add_stats(self,tic,toc,npred=0, nresults=0):
        etime = toc - tic

        try:
            self.sum_results[npred-1] += nresults
            self.sum_time[npred-1] += etime #/float(npred)
            self.max_time[npred-1]  = max(self.max_time[npred-1], etime)
            self.count[npred-1]    += 1
            self.t_count         += 1
        except IndexError:
            pass


    def query_stats(self):

        stats = []
        for mn, mx, nres, cnt in zip(self.sum_time, self.max_time, self.sum_results, self.count):
            stats.extend([mn/max(cnt,1), mx, nres/max(cnt,1), cnt])

        return tuple(stats)


cassandra_timeout = CONF.getValue('CassandraTimeout',10.0)
class CassandraError(Exception):
    """
    An exception class for ION Cassandra Client errors
    """


class CassandraStore(TCPConnection):
    """
    An Adapter class that implements the IStore interface by way of a
    cassandra client connection. As an adapter, this assumes an active
    client (it implements/provides no means of connection management).
    The same client instance could be used by another adapter class that
    implements another interface.
    
    @note: This is how we map the OOI architecture terms to Cassandra.  
    persistent_technology --> hostname, port
    persistent_archive --> keyspace
    cache --> columnfamily

    @todo Provide explanation of the cassandra options 
     - keyspace: Outermost context within a Cassandra server (like vhost).
     - column family: Like a database table. 
    """

    implements(store.IStore)

    put_stats = SizeStats()
    get_stats = SizeStats()
    has_stats = SizeStats()

    stats_out = 10000

    def __init__(self, persistent_technology, persistent_archive, credentials, cache):
        """
        functional wrapper around active client instance
        """
        ### Get the host and port from the Persistent Technology resource
        host = persistent_technology.hosts[0].host
        port = persistent_technology.hosts[0].port
        
        ### Get the Key Space for the connection
        self._keyspace = persistent_archive.name
        
        #Get the credentials for the cassandra connection
        log.info("CassandraStore.__init__")
        uname = credentials.username
        pword = credentials.password
        authorization_dictionary = {'username': uname, 'password': pword}
        log.info("Connecting to %s on port %s " % (host,port))
        log.info("Using keyspace %s" % (self._keyspace,))
        log.info("authorization_dictionary; %s" % (str(authorization_dictionary),))
        ### Create the twisted factory for the TCP connection  
        self._manager = ManagedCassandraClientFactory(keyspace=self._keyspace, credentials=authorization_dictionary)
        
        # Call the initialization of the Managed TCP connection base class
        TCPConnection.__init__(self,host, port, self._manager)
        self.client = CassandraClient(self._manager)    
        
        self._cache = cache # Cassandra Column Family maps to an ION Cache resource
        self._cache_name = cache.name
        log.info("leaving __init__")


    get_stats_string = 'Cassandra Store Get Stats(%d ops): time seconds (mean/mean per Kb/max) %f/%f/%f; size (mean/max) %f/%f Kb, Not Found - %d;'
    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def get(self, key):
        """
        @brief Return a value corresponding to a given key
        @param key 
        @retval Deferred that fires with the value of key
        """
        
        #log.debug("CassandraStore: Calling get on key %s " % key)

        tic = time.time()
        lval = 0
        try:
            result = yield self.client.get(key, self._cache_name, column='value')
            value = result.column.value

            lval = len(value)

        except NotFoundException:
            #log.info("Didn't find the key: %s. Returning None" % binascii.b2a_hex(key))
            value = None

        toc = time.time()

        self.get_stats.add_stats(tic,toc,lval)

        if self.get_stats.t_count >= self.stats_out:
            log.critical(self.get_stats_string % self.get_stats.get_stats())
            self.get_stats.__init__()

        defer.returnValue(value)

    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def put(self, key, value):
        """
        @brief Write a key/value pair into cassandra
        @param key Lookup key
        @param value Corresponding value
        @note Value is composed into OOI dictionary under keyname 'value'
        @retval Deferred for success
        """
        #log.debug("CassandraStore: Calling put on key: %s  value: %s " % (key, value))
        # @todo what exceptions need to be handled for an insert?

        tic = time.time()

        columns = {"value": value, "has_key":"1"}
        yield self.client.batch_insert(key, self._cache_name, columns)

        toc = time.time()

        self.put_stats.add_stats(tic,toc,len(value))

        if self.put_stats.t_count >= self.stats_out:
            log.critical('Cassandra Store Put Stats(%d ops): time seconds (mean/mean per Kb/max) %f/%f/%f; size (mean/max) %f/%f Kb;' % self.put_stats.put_stats())
            self.put_stats.__init__()

    has_key_stats_string = 'Cassandra Store Has_Key Stats(%d ops): time seconds (mean/max) %f/%f;'
    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def has_key(self, key):
        """
        Checks to see if the key exists in the column family
        @param key is the key to check in the column family
        @retVal Returns a bool in a deferred
        """
        tic = time.time()

        try:
            yield self.client.get(key, self._cache_name, column="has_key")
            ret = True
        except NotFoundException:
            ret = False

        toc = time.time()

        self.has_stats.add_stats(tic,toc)

        if self.has_stats.t_count >= self.stats_out:
            log.critical(self.has_key_stats_string % self.has_stats.simple_stats())
            self.has_stats.__init__()

        defer.returnValue(ret)

    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def remove(self, key):
        """
        @brief delete a key/value pair
        @param key Key to delete
        @retval Deferred, for success of operation
        @note Deletes are lazy, so key may still be visible for some time.
        """
        yield self.client.remove(key, self._cache_name)

    def on_deactivate(self, *args, **kwargs):
        #self._connector.disconnect()
        self._manager.shutdown()
        log.info('on_deactivate: Lose TCP Connection')

    def on_terminate(self, *args, **kwargs):
        log.info("Called CassandraStore.on_terminate")
        self._connector.disconnect()
        self._manager.shutdown()
        log.info('on_terminate: Lose TCP Connection')
    
    def on_error(self, *args, **kwargs):
        log.info("Called CassandraStore.on_error")
        self._connector.disconnect()
        self._manager.shutdown()
        log.info('on_error: Lose TCP Connection')


    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):

        yield TCPConnection.on_activate(self)




class CassandraIndexedStore(CassandraStore):
    """
    An Adapter class that provides the ability to use secondary indexes in Cassandra. It
    extends the IStore interface by adding a query and update_index method. It provides functionality
    for associating attributes with a value. These attributes are used in the query functionality. 
    """
    implements(store.IIndexStore)


    put_stats = SizeStats()
    get_stats = SizeStats()
    has_stats = SizeStats()
    update_stats = SizeStats()

    query_stats = QueryStats()

    has_key_stats_string = 'Cassandra Index Store Has_Key Stats(%d ops): time seconds (mean/max) %f/%f;'

    get_stats_string = 'Cassandra Index Store Get Stats(%d ops): time seconds (mean/mean per Kb/max) %f/%f/%f; size (mean/max) %f/%f Kb, Not Found - %d;'


    def __init__(self, persistent_technology, persistent_archive, credentials, cache):
        """
        functional wrapper around active client instance
        """   
        CassandraStore.__init__(self, persistent_technology, persistent_archive, credentials, cache)  
        self._query_attribute_names = None


    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def put(self, key, value, index_attributes=None):
        """
        Istore put, plus a dictionary of indexed stuff
        
        @param key The key to the Cassandra row
        @param value The value of the value column in the Cassandra row
        @param index_attributes The dictionary contains keys for the column name and the index value
        """

        tic = time.time()

        if index_attributes is None:
            index_cols = {}
        else:
            index_cols = dict(**index_attributes)

            
        #log.info("Put: index_attributes %s" % (index_cols))
        yield self._check_index(index_cols)
        index_cols.update({"value":value, "has_key":"1"})
        
        yield self.client.batch_insert(key, self._cache_name, index_cols)


        toc = time.time()
        self.put_stats.add_stats(tic,toc,len(value))

        if self.put_stats.t_count >= self.stats_out:
            log.critical('Cassandra Index Store Put Stats(%d ops): time seconds (mean/mean per Kb/max) %f/%f/%f; size (mean/max) %f/%f Kb;' % self.put_stats.put_stats())
            self.put_stats.__init__()

    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def update_index(self, key, index_attributes):
        """
        @brief Update the index attributes, but keep the value the same. 
        @param key The key to the row.
        @param index_attributes A dictionary of column names and values. These attributes
        can be used to query the store to return rows based on the value of the attributes.
        """
        tic = time.time()

        yield self._check_index(index_attributes)
        #log.info("Updating index for key %s attrs %s " % ( key, index_attributes))
        yield self.client.batch_insert(key, self._cache_name, index_attributes)
        defer.succeed(None)


        toc = time.time()

        self.update_stats.add_stats(tic,toc)

        if self.update_stats.t_count >= self.stats_out:
            log.critical('Cassandra Index Store Update_Index Stats(%d ops): time (mean/max) %f/%f seconds;' % self.update_stats.simple_stats())
            self.update_stats.__init__()



    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def _check_index(self, index_attributes):
        """
        Ensure that the index_attribute keys are columns that are indexed in the column family.
        Ensure that the column values are of type str.
        
        This method raises an IndexStoreError exception if the index_attribute dictionary has keys that 
        are not the names of the columns indexed. 
        """
        #Get the set of indexes the first time this is called.
        if self._query_attribute_names is None:
            query_attributes =  yield self.get_query_attributes()
            self._query_attribute_names = set(query_attributes)


        index_attribute_names = set(index_attributes.keys())
        
        if not index_attribute_names.issubset(self._query_attribute_names):
            bad_attrs = index_attribute_names.difference(self._query_attribute_names)
            raise IndexStoreError("These attributes: %s %s %s"  % (",".join(bad_attrs),os.linesep,"are not indexed."))
        
                
        isstr = lambda x: isinstance(x, (str,unicode))
        alltrue = lambda x,y: x and y
        all_strings = reduce(alltrue, map(isstr, index_attributes.values()), True)
        if not all_strings:
            raise IndexStoreError("Values for the indexed columns must be of type str.")
        

    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def query(self, query_predicates, row_count=10000000):
        """
        Search for rows in the Cassandra instance.
    
        @param query_predicates is an instance of store.Query. 
        @param row_count the maximum number of rows to return. 
        The default argument is set to 10,000,000. 
        (Setting this sys.maxint causes an internal error in Cassandra.)
        This can be set  to a lower value, if you want to limit the number of rows 
        to return.
            
        @retVal a dictionary containing the keys and values which match the query.
        
        raises a CassandraError if the query_predicate object is malformed.
        """
        #log.info('Query against cache: %s' % self._cache_name)

        tic = time.time()


        predicates = query_predicates.get_predicates()
        def fix_preds(query_tuple):
            if query_tuple[2] == Query.EQ:
                new_pred = IndexOperator.EQ
            elif query_tuple[2] == Query.GT:
                new_pred = IndexOperator.GT
            else:
                raise CassandraError("Illegal predicate value")
            args = {'column_name':query_tuple[0], 'op':new_pred, 'value': query_tuple[1]}
            return IndexExpression(**args)
        selection_predicates = map(fix_preds, predicates)
        #log.debug("Calling get_indexed_slices selection_predicate %s " % (selection_predicates,))
        
        rows = yield self.client.get_indexed_slices(self._cache_name, selection_predicates, count=row_count)
        #log.info("Got rows back")
        result ={}
        for row in rows:
            row_vals = {}
            for column in row.columns:
                row_vals[column.column.name] = column.column.value
            result[row.key] = row_vals


        toc = time.time()


        self.query_stats.add_stats(tic,toc,len(predicates), len(rows))

        if self.query_stats.t_count >= (self.stats_out/10):
            log.critical('Cassandra Index Store Query Time(seconds) Stats per predicate (mean/max/mean # of rows/cnt): 1 - %f/%f/%f/%d; 2 - %f/%f/%f/%d; 3 - %f/%f/%f/%d;  4 - %f/%f/%f/%d;  5 - %f/%f/%f/%d;' % self.query_stats.query_stats())
            self.query_stats.__init__()

        defer.returnValue(result)
        
    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def get_query_attributes(self):
        """
        Return the column names that are indexed.
        """
        log.warn('Calling get_query_attributes - this is expensive!')
        keyspace_description = yield self.client.describe_keyspace(self._keyspace)
        #log.debug("keyspace desc %s" % (keyspace_description,))
        get_cfdef = lambda cfdef: cfdef.name == self._cache_name
        cfdef = filter(get_cfdef, keyspace_description.cf_defs)
        get_names = lambda cdef: cdef.name
        indexes = map(get_names, cfdef[0].column_metadata)
        
        
        defer.returnValue(indexes)




class CassandraStorageResource:
    """
    This class holds the connection information in the
    persistent_technology, persistent_archive, cache, and 
    credentials
    """
    def __init__(self, persistent_technology, persistent_archive=None, cache=None, credentials=None):
        self.persistent_technology = persistent_technology
        self.persistent_archive = persistent_archive
        self.cache = cache
        self.credentials = credentials
        
    def get_host(self):
        return self.persistent_technology.hosts[0].host
    
    def get_port(self):
        return self.persistent_technology.hosts[0].port
    
    def get_credentials(self):    
        uname = self.credentials.username
        pword = self.credentials.password
        authorization_dictionary = {'username': uname, 'password': pword}
        return authorization_dictionary
    

class CassandraDataManager(TCPConnection):

    #implements(store.IDataManager)

    def __init__(self, storage_resource):
        """
        @param storage_resource provides the connection information to connect to the Cassandra cluster.
        """
        host = storage_resource.get_host()
        port = storage_resource.get_port()
        authorization_dictionary = storage_resource.get_credentials()
        log.info("host: %s and port: %s" % (host,str(port)))
        self._manager = ManagedCassandraClientFactory(credentials=authorization_dictionary)
        
        TCPConnection.__init__(self,host,port,self._manager)
        self.client = CassandraClient(self._manager)    
        
    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def create_persistent_archive(self, persistent_archive):
        """
        @brief Create a Cassandra Keyspace
        @param persistent_archive is an ion resource which defines the properties of a Key Space
        """
        keyspace = persistent_archive.name
        log.info("Creating keyspace with name %s" % (keyspace,))
        #Check to see if replication_factor and strategy_class is defined in the persistent_archive
        ksdef = KsDef(name=keyspace, replication_factor=1,
                strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                cf_defs=[])
        yield self.client.system_add_keyspace(ksdef)
        
        log.info("Added and set keyspace")

    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def update_persistent_archive(self, persistent_archive):
        """
        @brief Update a Cassandra Keyspace
        This method should update the Key Space properties - not change the column families!
        @param persistent_archiveis a persistent archive object which defines the properties of a Key Space
        """
        pa = persistent_archive
        ksdef = KsDef(name=pa.name,replication_factor=pa.replication_factor,
                      strategy_class=pa.strategy_class, cf_defs=[])
        yield self.client.system_update_keyspace(ksdef)
        
    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def remove_persistent_archive(self, persistent_archive):
        """
        @brief Remove a Cassandra Key Space
        @param persistent_archive is a persistent archive object which defines the properties of a Key Space
        """
        keyspace = persistent_archive.name
        log.info("Removing keyspace with name %s" % (keyspace,))
        yield self.client.system_drop_keyspace(keyspace)
        
    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def create_cache(self, persistent_archive, cache):
        """
        @brief Create a Cassandra column family
        @param persistent_archive is a persistent archive object which defines the properties of an existing Key Space
        @param cache is a cache object which defines the properties of column family
        """
        yield self.client.set_keyspace(persistent_archive.name)
        cfdef = CfDef(keyspace=persistent_archive.name, name=cache.name)
        yield self.client.system_add_column_family(cfdef)
    
    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def remove_cache(self, persistent_archive, cache):
        """
        @brief Remove a Cassandra column family
        @param persistent_archive is a persistent archive object which defines the properties of an existing Key Space
        @param cache is a cache object which defines the properties of column family
        """
        yield self.client.set_keyspace(persistent_archive.name)
        yield self.client.system_drop_column_family(cache.name)

    @timeout(cassandra_timeout)
    @defer.inlineCallbacks
    def update_cache(self, persistent_archive, cache):
        """
        @brief Update a Cassandra column family
        @param persistent_archive is a persistent archive object which defines the properties of an existing Key Space
        @param cache is a cache object which defines the properties of column family
        
        @note This update operation handles only one column_metadata gpb object. It needs to be generalized to work
        with more than one. 
        """
        yield self.client.set_keyspace(persistent_archive.name)
        desc = yield self.client.describe_keyspace(persistent_archive.name)
        log.info("Describe keyspace return %s" % (desc,))
        #Retrieve the correct column family by filtering by name
        select_cf = lambda cf_name: cf_name.name == cache.name
        cf_defs = filter(select_cf, desc.cf_defs)
        #Raise an exception if it doesn't find the column family
        assert len(cf_defs) == 1
        cf_id = cf_defs[0].id
        log.info("Update column family with %s,%s,%s,%s%s" % (persistent_archive.name, cache.name, cf_id, cache.column_type, cache.comparator_type))
        
        column = cache.column_metadata[0]
        log.info("column attrs %s " % (column.__dict__))
        log.info("Column message fields: %s,%s,%s" % (column.column_name,column.validation_class, column.index_name))

        cf_column_metadata = self.__generate_column_metadata(cache)
        cf_def = CfDef(keyspace = persistent_archive.name,
                       name = cache.name,
                       id=cf_id,
                       column_type=cache.column_type,
                       comparator_type=cache.comparator_type,
                       column_metadata= cf_column_metadata)   
        log.info("cf_def: " + str(cf_def))      
        yield self.client.system_update_column_family(cf_def) 
    
    @defer.inlineCallbacks
    def _describe_keyspace(self, keyspace):
        """    
        @brief internal method used to get a description of the keyspace
        @param keyspace is a string of the keyspace name
        @retval returns a thrift KsDef 
        """
        desc = yield self.client.describe_keyspace(keyspace)
        defer.returnValue(desc)
        
    @defer.inlineCallbacks
    def _describe_keyspaces(self):
        """
        @brief internal method used to get a description of all keyspaces 
        in the cluster.
        @retval returns a list of thrift KsDefs
        """
        log.info("In CassandraDataManager._describe_keyspaces")
        desc = yield self.client.describe_keyspaces()
        defer.returnValue(desc)
    
    def __generate_column_metadata(self, column_family):
        """
        Convenience method that generates a list of ColumnDefs from the column_metadata fields
        """
        args_name = ["name","validation_class", "index_type", "index_name"]
        #Generate a list of args foreach column definition
        args_func = lambda x: [x.column_name,x.validation_class, x.index_type , x.index_name ]
        cdefs_args = map(args_func, column_family.column_metadata)
        #Generate a dictionary of args for each column definition
        make_args_dict = lambda x: dict(zip(args_name, x))
        cdefs_dicts = map(make_args_dict, cdefs_args)
        #Create the ColumnDef for each kwarg dictionary
        make_cdefs = lambda d: ColumnDef(**d)
        cdefs = map(make_cdefs, cdefs_dicts)
        return cdefs
        
        
    def on_deactivate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_deactivate: Lose Connection TCP')

    def on_terminate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_terminate: Lose Connection TCP')



