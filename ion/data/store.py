#!/usr/bin/env python

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

import re
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer


class IStore(object):
    """
    Interface and abstract base class for all store backend implementations.
    All operations are returning deferreds and operate asynchronously.

    @note Pure virtual abstract base class - must override methods!
    """
    def __init__(self, **kwargs):
        """
        @brief Initializes store instance
        @param kwargs arbitrary keyword arguments interpreted by the subclass
        """
        pass

    @classmethod
    def create_store(cls, **kwargs):
        """
        @brief Factory method to create an instance of the store.
        @param kwargs arbitrary keyword arguments interpreted by the subclass to
                configure the store.
        @retval Deferred, for IStore instance.
        """
        instance = cls(**kwargs)
        instance.kwargs = kwargs
        return defer.succeed(instance)

    def clear_store(self):
        """
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
        

    def get(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for value associated with key, or None if not existing.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def put(self, key, value):
        """
        @param key  an immutable key to be associated with a value
        @param value  an object to be associated with the key. The caller must
                not modify this object after it was
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def query(self, regex):
        """
        @param regex  regular expression matching zero or more keys
        @retval Deferred, for list of values for keys matching the regex
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def remove(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"


class Store(IStore):
    """
    Memory implementation of an asynchronous key/value store, using a dict.
    """
    def __init__(self, **kwargs):
        self.kvs = {}

    def clear_store(self):
        self.kvs = {}
        return defer.succeed(None)

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

    def query(self, regex):
        """
        @see IStore.query
        """
        return defer.maybeDeferred(self._query, regex)

    def _query(self, regex):
        #keys = [re.search(regex,m).group() for m in self.kvs.keys() if re.search(regex,m)]
        match_list=[]
        for s in self.kvs.keys():
            #m = re.search(regex, s)
            m = re.findall(regex, s)
            if m:
                match_list.extend(m)
        return match_list

    def remove(self, key):
        """
        @see IStore.remove
        """
        # could test for existance of key. this will error otherwise
        if self.kvs.has_key(key):
            del self.kvs[key]
        return defer.succeed(None)
