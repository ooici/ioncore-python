#!/usr/bin/env python

"""
@file ion/data/store.py
@author Michael Meisinger
@brief base interface for all key-value stores in the system and default
        in memory implementation
"""

import re
from twisted.internet import defer

class IStore(object):
    """
    Interface for all store backend implementations. All operations are returning
    deferreds and operate asynchronously.
    """
    def get(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for value associated with key, or None if not existing.
        """

    def read(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def put(self, key, value):
        """
        @param key  an immutable key to be associated with a value
        @param value  an object to be associated with the key. The caller must
                not modify this object after it was
        @retval Deferred, for success of this operation
        """

    def write(self, *args, **kwargs):
        return self.put(*args, **kwargs)

    def query(self, regex):
        """
        @param regex  regular expression matching zero or more keys
        @retval Deferred, for list of values for keys matching the regex
        """

    def delete(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
        """

    def init(self, **kwargs):
        """
        Configures this Store with arbitrary keyword arguments
        @param kwargs  any keyword args
        @retval Deferred, for success of this operation
        """

class Store(IStore):
    """
    Memory implementation of an asynchronous store, based on a dict.
    """
    def __init__(self):
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
        return
