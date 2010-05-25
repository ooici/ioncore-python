#!/usr/bin/env python

"""
@file ion/data/store.py
@author Michael Meisinger
@brief base interface for all key-value stores in the system and default
        in memory implementation
"""

import re
import logging

from twisted.internet import defer


class IStore(object):
    """
    Interface and abstract base class for all store backend implementations.
    All operations are returning deferreds and operate asynchronously.
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

    def get(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for value associated with key, or None if not existing.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def read(self, *args, **kwargs):
        """
        Inheritance safe alias for get
        """
        return self.get(*args, **kwargs)

    def put(self, key, value):
        """
        @param key  an immutable key to be associated with a value
        @param value  an object to be associated with the key. The caller must
                not modify this object after it was
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def write(self, *args, **kwargs):
        """
        Inheritance safe alias for put
        """
        return self.put(*args, **kwargs)

    def query(self, regex):
        """
        @param regex  regular expression matching zero or more keys
        @retval Deferred, for list of values for keys matching the regex
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def delete(self, key):
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
        return [re.search(regex,m).group() for m in self.kvs.keys() if re.search(regex,m)]

    def delete(self, key):
        """
        @see IStore.delete
        """
        return defer.maybeDeferred(self._delete, key)

    def _delete(self, key):
        del self.kvs[key]

