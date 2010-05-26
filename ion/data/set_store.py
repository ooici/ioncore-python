#!/usr/bin/env python

"""
@file ion/data/set_store.py
@package ion.data.ISetStore Pure virtual base class for Sets
@package ion.data.SetStore In-memory implementation of ion.data.ISetStore
@author Michael Meisinger
@brief base interface for all key-value stores in the system and default
        in memory implementation
"""

import re
import logging

from twisted.internet import defer


class ISetStore(object):
    """
    Interface and abstract base class for all store backend implementations.
    All operations are returning deferreds and operate asynchronously.

    @note Pure virtual abstract base class - must override methods!
    """
    def __init__(self, **kwargs):
        """
        @brief Initializes SetStore instance
        @param kwargs arbitrary keyword arguments interpreted by the subclass
        """
        pass

    @classmethod
    def create_set_store(cls, **kwargs):
        """
        @brief Factory method to create an instance of the store.
        @param kwargs arbitrary keyword arguments interpreted by the subclass to
                configure the store.
        @retval Deferred, for ISetStore instance.
        """
        instance = cls(**kwargs)
        instance.kwargs = kwargs
        return defer.succeed(instance)

    def smembers(self, key):
        """
        @param key a key associated with a set
        @retval Deferred, set associated with key, or None if not existing.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    #def read(self, *args, **kwargs):
    #    """
    #    Inheritance safe alias for get
    #    """
    #    return self.get(*args, **kwargs)

    def sadd(self, key, member):
        """
        @brief Add a member to the set stored at key
        @param key Lookup key
        @param member The member to add in the set
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    #def write(self, *args, **kwargs):
    #    """
    #    Inheritance safe alias for put
    #    """
    #    return self.put(*args, **kwargs)

    def sremove(self, key, member):
        """
        @brief delete a member from the set at key
        @param key for the set
        @param member, the member to remove from the set
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def scard(self, key):
        """
        @brief return the number of elements in the set
        @param key which is mapped to the set
        @retval Deferred, integer representing the the cardinality of the set
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"
    
    def query(self, regex):
        """
        @param regex  regular expression matching zero or more keys
        @retval Deferred, for list of sets for keys matching the regex
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def delete(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred, for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"


class SetStore(ISetStore):
    """
    Memory implementation of an asynchronous key/value store, using a dict.
    """
    def __init__(self, **kwargs):
        self.kss = {}

    def smembers(self, key):
        """
        @see ISetStore.smembers
        """
        return defer.maybeDeferred(self.kss.get, key, None)

    def sadd(self, key, member):
        """
        @see ISetStore.sadd
        """
        return defer.maybeDeferred(self._sadd, key, member)

    def _sadd(self, key, member):
        if key in self.kss:
            self.kss[key].add(member)
        else:
            self.kss[key]=set([member])

    def sremove(self, key, member):
        """
        @see ISetStore.sremove
        """
        return defer.maybeDeferred(self._sremove, key, member)

        
    def _sremove(self, key, member):
        if self.kss.has_key(key):
            self.kss[key].discard(member)

    def scard(self, key):
        """
        @see ISetStore.scard
        """
        return defer.maybeDeferred(self._scard, key)

    def _scard(self, key):
        """
        @see ISetStore.scard
        """
        if self.kss.has_key(key):
            return len(self.kss[key])
        return 0

    def query(self, regex):
        """
        @see ISetStore.query
        """
        return defer.maybeDeferred(self._query, regex)

    def _query(self, regex):
        return [re.search(regex,m).group() for m in self.kss.keys() if re.search(regex,m)]

    def delete(self, key):
        """
        @see IStore.delete
        """
        return defer.maybeDeferred(self._delete, key)

    def _delete(self, key):
        if self.kss.has_key(key):
            del self.kss[key]







