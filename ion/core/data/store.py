"""
@file ion/data/store.py
@package ion.data.IStore Pure virtual base class for CRUD
@package ion.data.Store In-memory implementation of ion.data.IStore
@author Michael Meisinger
@author David Stuebe
@author Dorian Raymer
@brief base interface for all key-value stores in the system and default
        in memory implementation
"""
import os
from zope.interface import Interface
from zope.interface import implements

from twisted.internet import defer


import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)



class IStore(Interface):
    """
    Interface all store backend implementations.
    All operations are returning deferreds and operate asynchronously.

    @var namespace
    """

    def get(key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for value associated with key, or None if not existing.
        """

    def put(key, value):
        """
        @param key  an immutable key to be associated with a value
        @param value  an object to be associated with the key. The caller must
                not modify this object after it was
        @retval Deferred, for success of this operation
        """

    def remove(key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
     
        """

class Store(object):
    """
    Memory implementation of an asynchronous key/value store, using a dict.
    Simulates typical usage of using a client connection to a backend
    technology.
    """
    implements(IStore)

    def __init__(self, *args, **kwargs):
        self.kvs = {}

    def get(self, key):
        """
        @see IStore.get
        """
        return defer.maybeDeferred(self.kvs.get, key, None)

    def put(self, key, value):
        """
        @see IStore.put
        """
        return defer.maybeDeferred(self.kvs.update, {key:value})

    def remove(self, key):
        """
        @see IStore.remove
        """
        # could test for existance of key. this will error otherwise
        if self.kvs.has_key(key):
            del self.kvs[key]
        return defer.succeed(None)
    
    def has_key(self, key):
        """
        Checks to see if the key exists in the column family
        @param key is the key to check in the column family
        @retVal Returns a bool in a deferred
        """  
        return defer.maybeDeferred(self.kvs.has_key, key )

class IIndexStore(IStore):
    """
    Interface all store backend implementations.
    All operations are returning deferreds and operate asynchronously.

    @var namespace
    """

    def get(key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for value associated with key, or None if not existing.
        """

    def put(key, value, index_attributes={}):
        """
        @param key  an immutable key to be associated with a value
        @param value  an object to be associated with the key. The caller must
                not modify this object after it was
        @param index_attributes a dictionary of attributes by which to index this value of this key
        @retval Deferred, for success of this operation
        """

    def update_index(key, index_attributes):
        """
        @param key  an immutable key associated with a value
        @param index_attributes an update to the dictionary of attributes by which to index this value of this key
        """

    def remove(key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
     
        """
        
    def query(self, indexed_attributes_eq={}, indexed_attributes_gt={}):
        """
        Search for rows in the Cassandra instance.
    
        @param indexed_attributes is a dictionary with column:value mappings.
        Rows are returned that have columns set to the value specified in 
        the dictionary
        
        @retVal a thrift representation of the rows returned by the query.
        """
        
    def get_query_attributes(self):
        """
        Return the column names that are indexed.
        """

class IndexStoreError(Exception):
    """
    An exception class for the index store
    """

class IndexStore(object):
    """
    Memory implementation of an asynchronous key/value store, using a dict.
    Simulates typical usage of using a client connection to a backend
    technology.
    
    @note
    self.kvs is a dictionary of dictionaries where the keys are row keys and
    the values are a dictionary representing the columns. The 
        
        { key_1:  {name_1:val_1, name_2:val2_1, ... name_n:val_n ,
          key_2: {name_1:val_1, name_2:val2_1, ... name_n:val_n ,
          ...
          key_n: {name_1:val_1, name_2:val2_1, ... name_n:val_n }
    
    self.indices is an index to map attribute names to attribute values to keys
        {attr_names:{attr_value: set( keys)}}.
    """
    implements(IIndexStore)

    def __init__(self, *args, **kwargs):
        self.kvs = {}
        self.indices = {}
        
        if kwargs.has_key('indices'):
            for name in kwargs.get('indices'):
                self.indices[name]={}

    def get(self, key):
        """
        @see IStore.get
        """
        row = self.kvs.get(key, None)
        if row is None:
            return defer.succeed(None)
        else:
            return defer.maybeDeferred(row.get, "value")


    def _update_index(self, key, index_attributes):
        log.debug("In _update_index")
        log.debug("key %s index_attributes %s" % (key,index_attributes))
        #Ensure that we are updating attributes that are indexed.
        query_attribute_names = set(self.indices.keys())
        index_attribute_names = set(index_attributes.keys())
        
        if not index_attribute_names.issubset(query_attribute_names):
            bad_attrs = index_attribute_names.difference(query_attribute_names)
            raise IndexStoreError("These attributes: %s %s %s"  % (",".join(bad_attrs),os.linesep,"are not indexed."))
        
        for k, v in index_attributes.items():
            kindex = self.indices.get(k, None)
            if not kindex:
                kindex = {}
                self.indices[k] = kindex
            # Create a set of keys if it does not already exist
            kindex[v] = kindex.get(v, set())
            kindex[v].add(key)

    def put(self, key, value, index_attributes={}):
        """
        @see IStore.put
        Raises an exception if index_attibutes contains attributes that are not indexed
        by the underlying store.
        """
        self._update_index(key, index_attributes)
                        
        return defer.maybeDeferred(self.kvs.update, {key: dict({"value":value},**index_attributes)})
    
    
    def update_index(self, key, index_attributes):
        """
        @brief Update the index attributes, but keep the value the same. 
        @param key The key to the row.
        @param index_attributes A dictionary of column names and values. These attributes
        can be used to query the store to return rows based on the value of the attributes.
        
        Raises an IndexStoreException if you try to update an attribute that is not indexed.
        """
        log.debug("In update_index")
        self._update_index(key, index_attributes)
        self.kvs[key].update(index_attributes)
        return defer.succeed(None)
        
    
    def remove(self, key):
        """
        @see IStore.remove
        """
        # could test for existence of key. this will error otherwise
        if self.kvs.has_key(key):
            del self.kvs[key]            
        return defer.succeed(None)
        
    def query(self, query_predicates):
        """
        Search for rows in the Cassandra instance.
    
        @param indexed_attributes is a dictionary with column:value mappings.
        Rows are returned that have columns set to the value specified in 
        the dictionary
        
        @retVal A data structure representing Cassandra rows. See the class
        docstring for the description of the data structure.
        """
        predicates = query_predicates.get_predicates()
        
        eq_filter = lambda x: x[2] == Query.EQ
        preds_eq = filter(eq_filter, predicates)
        keys = set()
        if len(preds_eq) == 0:
            raise IndexStoreError('Invalid arguments to IndexStore - must provide at least one equal to operator for search!')
        else:
            k,v,pred = preds_eq.pop()
            kindex = self.indices.get(k, None)
            if kindex:
                keys.update(kindex.get(v,set()))
        
        for k,v,p in predicates:
            kindex = self.indices.get(k,None)
            if p == Query.EQ:
                
                if kindex:
                    keys.intersection_update(kindex.get(v,set()))
            elif p == Query.GT:
                
                matches = set()
                for attr_val in kindex.keys():
                    if attr_val > v:
                        matches.update(kindex.get(attr_val,set()))
                keys.intersection_update(matches)
        
        log.info("keys: "+ str(keys))
        result = {}
        for k in keys:
            # This is stupid, but now remove effectively works - delete keys are no longer visible!
            if self.kvs.has_key(k):
                result[k] = self.kvs.get(k)
                
        return defer.succeed(result)                

    def get_query_attributes(self):
        """
        Return the column names that are indexed.
        """
        return defer.maybeDeferred(self.indices.keys)
    
    def has_key(self, key):
        """
        Checks to see if the key exists in the column family
        @param key is the key to check in the column family
        @retVal Returns a bool in a deferred
        """  
        return defer.maybeDeferred(self.kvs.has_key, key)

class Query:
    """
    Class that holds the predicates used to query an IndexStore.
    """
    
    EQ = "EQ"
    GT = "GT"
    def __init__(self):
        self._predicates = []

    def add_predicate_eq(self, name, value):
        self._predicates.append((name,value,Query.EQ))
    
    def add_predicate_gt(self, name, value):
        self._predicates.append((name,value,Query.GT))
        
    def get_predicates(self):
        return self._predicates    
        
    

    
class IDataManager(Interface):
    """
    @note Proposed class to fulfill preservation service management?
    @brief Administrative functionality for backend store configuration. 
    """
    def create_persistent_archive(self, persistent_archive):
        """
        @brief Create a separate organizational instance in the backend
        @param persistent_archive is the name of the organization
        @retval succeed or fail
        """
        
        
    def remove_persistent_archive(self, persistent_archive):
        """
        @brief Remove an organizational instance in the backend
        @param persistent_archive is the name of the organization
        """
        
    def update_persistent_archive(self, persistent_archive):
        """
        @brief changes the configuration of the persistent archive
        @param persistent_archive the name and configuration of the persistent archive.
        This is represented as an OOI resource.
        """
        
    def create_cache(self, persistent_archive, cache):
        """
        @brief creates a new cache in Cassandra this creates a new column family
        @param persistent_archive the archive in which the cache resides
        @param cache a resource representation of the cache, this includes its name and configuration
        """
    
    def update_cache(self, cache):
        """
        @brief changes the configuration of the current cache
        @param a resource representation of the cache
        """
        
    def remove_cache(self, cache):
        """
        @brief remove the current cache
        @param a resource representation of the cache
        """



class DataManager(object):
    """
    Memory implementation of the IDataManager interface. This conforms to the interface,
    but does nothing.
    """
    implements(IDataManager)

    def create_persistent_archive(self, persistent_archive):
        """
        @brief Create a separate organizational instance in the backend
        @param persistent_archive the name and configuration of the persistent archive.
        @retval succeed or fail
        """
        
        
    def remove_persistent_archive(self, persistent_archive):
        """
        @brief Remove an organizational instance in the backend
        @param persistent_archive is the name of the organization
        """
        
    def update_persistent_archive(self, persistent_archive):
        """
        @brief changes the configuration of the persistent archive
        @param persistent_archive the name and configuration of the persistent archive.
        This is represented as an OOI resource.
        """
        
    def create_cache(self, persistent_archive, cache):
        """
        @brief creates a new cache in Cassandra this creates a new column family
        @param persistent_archive the archive in which the cache resides
        @param cache a resource representation of the cache, this includes its name and configuration
        """
    
    def update_cache(self, cache):
        """
        @brief changes the configuration of the current cache
        @param a resource representation of the cache
        """
        
    def remove_cache(self, persistent_archive, cache):
        """
        @brief remove the current cache
        @param persistent_archive the name and configuration of the persistent archive.
        @param a resource representation of the cache
        """
    
    
    
class BackendBuilder(object):
    """
    All store client connections need:
        - host
        - port
    All stores have:
        - namespace

    See if a generic process TCP connector makes sense.
    Any implementation of IStore must operate in the ion framework, and
    therefore it only makes sense for the life cycle of the class instance
    and the connection of the backend client to be carried out in concert
    with an ion process.
    """

    def __init__(self, host, port, process):
        """
        @param process the process instance
        """
        self.host = host
        self.port = port
        self.process = process
        
        






