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
        @retval Deferred for ISetStore instance.
        """
        instance = cls(**kwargs)
        instance.kwargs = kwargs
        return defer.succeed(instance)

    def smembers(self, key):
        """
        @param key a key associated with a set
        @retval Deferred set associated with key, or empty set if not existing.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def srandmember(self, key):
        """
        @brief Return a random member from the set key.
        @param key set to get random member from.
        @retval Deferred that fires with member or None if empty set.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def sadd(self, key, member):
        """
        @brief Add a member to the set stored at key
        @param key Lookup key
        @param member The member to add in the set
        @retval Deferred for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def sremove(self, key, member):
        """
        @brief delete a member from the set at key
        @param key for the set
        @param member the member to remove from the set
        @retval Deferred for success of this operation
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def spop(self, key):
        """
        @brief Remove and return an arbitrary member of set at key
        @param key key of the set
        @retval Deferred that fires with set member or None if set is empty
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def smove(self, srckey, dstkey, member):
        """
        @brief Move the specified member from the set at srckey to the set
        at dstkey
        @param srckey key of source set
        @param dstkey key of destination set
        @param member The set member to move
        @retval Deferred that fires with True if member is moved and False
        if member was not in the set at srckey
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def scard(self, key):
        """
        @brief return the number of elements in the set
        @param key which is mapped to the set
        @retval Deferred integer representing the the cardinality of the set
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def smember(self, key, member):
        """
        @brief Test if member exists in set stored at key
        @param key of the set
        @param member element to test membership of
        @retval Deferred that fires with True if member is in the set, False otherwise.
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def sinter(self, key, *keys):
        """
        @brief return the members resulting in the intersection of sets at
        provided keys.
        @param key key of set to intersect
        @param keys more keys
        @retval Deferred that fires with a set of elements
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def sinterstore(self, dstkey, key, *keys):
        """
        @brief Same as sinter, except resulting set is stored as dstkey.
        @param dstkey key of set where result of intersection is stored.
        @param key set[s] to intersect.
        @retval Deferred that fires with None.
        @todo XXX what should be returned?
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def sunion(self, key, *keys):
        """
        @brief return the members resulting in the union of sets at
        provided keys.
        @param key key of set to union
        @param keys more keys
        @retval Deferred that fires with a set of elements
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def sunionstore(self, dstkey, key, *keys):
        """
        @brief Same as sunion, except result is stored as dstkey.
        @param dstkey key of set where result of intersection is stored.
        @param key key of set to union
        @param keys more keys
        @retval Deferred that fires with None
        @todo XXX what should be returned?
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def sdiff(self, key, *keys):
        """
        @brief return the members resulting in the difference between the
        first set and all successive sets
        @param key The set from which the successive sets subtract members
        from
        @param keys The successive sets.
        @retval Deferred that fires with a set of elements
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def sdiffstore(self, dstkey, key, *keys):
        """
        @brief Same as sdiff, but result of difference is stored as dstkey.
        @param key The set from which the successive sets subtract members
        from
        @param keys The successive sets.
        @retval Deferred that fires with None
        @todo XXX what should be returned?
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def query(self, regex):
        """
        @param regex  regular expression matching zero or more keys
        @retval Deferred for list of sets for keys matching the regex
        """
        raise NotImplementedError, "Abstract Interface Not Implemented"

    def remove(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred for success of this operation
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
        @param key a key associated with a set
        @retval Deferred set associated with key, or empty set if not existing.
        """
        return defer.maybeDeferred(self._sinter, key)

    def srandmember(self, key):
        """
        @brief Return a random member from the set key.
        @param key set to get random member from.
        @retval Deferred that fires with member or None if empty set.
        """
        return defer.maybeDeferred(self._srandmember, key)

    def _srandmember(self, key):
        try:
            elm = self.kss[key].pop()
            self.kss[key].add(elm)
            return elm
        except KeyError:
            return None

    def sadd(self, key, member):
        """
        @brief Add a member to the set stored at key
        @param key Lookup key
        @param member The member to add in the set
        @retval Deferred, for success of this operation
        """
        return defer.maybeDeferred(self._sadd, key, member)

    def _sadd(self, key, member):
        if key in self.kss:
            self.kss[key].add(member)
        else:
            self.kss[key]=set([member])

    def spop(self, key):
        """
        @brief Remove and return an arbitrary member of set at key
        @param key key of the set
        @retval Deferred that fires with set member or None if set is empty
        """
        return defer.maybeDeferred(self._spop, key)

    def _spop(self, key):
        if self.kss.has_key(key):
            return self.kss[key].pop()
        return None

    def smove(self, srckey, dstkey, member):
        """
        @brief Move the specified member from the set at srckey to the set
        at dstkey
        @param srckey key of source set
        @param dstkey key of destination set
        @param member The set member to move
        @retval Deferred that fires with True if member is moved and False
        if member was not in the set at srckey
        """
        return defer.maybeDeferred(self._smove, srckey, dstkey, member)

    def _smove(self, srckey, dstkey, member):
        if self.kss.has_key(srckey):
            if member in self.kss[srckey]:
                self.kss[srckey].discard(member)
                self._sadd(dstkey, member)
                return True
        return False

    def sremove(self, key, member):
        """
        @brief delete a member from the set at key
        @param key for the set
        @param member the member to remove from the set
        @retval Deferred for success of this operation
        """
        return defer.maybeDeferred(self._sremove, key, member)

    def _sremove(self, key, member):
        if self.kss.has_key(key):
            self.kss[key].discard(member)

    def scard(self, key):
        """
        @brief return the number of elements in the set
        @param key which is mapped to the set
        @retval Deferred integer representing the the cardinality of the set
        """
        return defer.maybeDeferred(self._scard, key)

    def _scard(self, key):
        """
        @see ISetStore.scard
        """
        return len(self.kss.get(key, set()))

    def sismember(self, key, member):
        """
        @brief Test if member exists in set stored at key
        @param key of the set
        @param member element to test membership of
        @retval True if member is in the set. False otherwise.
        """
        return defer.maybeDeferred(self._sismember, key, member)

    def _sismember(self, key, member):
        if self.kss.has_key(key):
            return member in self.kss[key]
        return False

    def sinter(self, key, *keys):
        """
        @brief return the members resulting in the intersection of sets at
        provided keys.
        @param key key of set to intersect
        @param keys more keys
        @retval Deferred that fires with a set of elements
        """
        return defer.maybeDeferred(self._sinter, key, *keys)

    def _sinter(self, key, *keys):
        try:
            cur_set = self.kss[key]
        except KeyError:
            # Intersection with an empty set is always an empty set
            return set()
        for k in keys:
            try:
                next_set = self.kss[k]
            except KeyError:
                # Intersection with an empty set is always an empty set
                return set()
            cur_set = cur_set.intersection(next_set)
        return cur_set

    def sinterstore(self, dstkey, key, *keys):
        """
        @brief Same as sinter, except resulting set is stored as dstkey.
        @param dstkey key of set where result of intersection is stored.
        @param key set[s] to intersect.
        @retval Deferred that fires with None.
        @todo XXX what should be returned?
        """
        def _store():
            res_set = self._sinter(key, *keys)
            self.kss[dstkey] = res_set
            return None
        return defer.maybeDeferred(_store)

    def sunion(self, key, *keys):
        """
        @brief return the members resulting in the union of sets at
        provided keys.
        @param key key of set to union
        @param keys more keys
        @retval Deferred that fires with a set of elements
        """
        return defer.maybeDeferred(self._sunion, key, *keys)

    def _sunion(self, key, *keys):
        cur_set = self.kss.get(key, set())
        for k in keys:
            next_set = self.kss.get(k, set())
            cur_set = cur_set.union(next_set)
        return cur_set

    def sunionstore(self, dstkey, key, *keys):
        """
        @brief Same as sunion, except result is stored as dstkey.
        @param dstkey key of set where result of intersection is stored.
        @param key key of set to union
        @param keys more keys
        @retval Deferred that fires with None
        @todo XXX what should be returned?
        """
        def _store():
            res_set = self._sunion(key, *keys)
            self.kss[dstkey] = res_set
            return None
        return defer.maybeDeferred(_store)

    def sdiff(self, key, *keys):
        """
        @brief return the members resulting in the difference between the
        first set and all successive sets
        @param key The set from which the successive sets subtract members
        from
        @param keys The successive sets.
        @retval Deferred that fires with a set of elements
        """
        return defer.maybeDeferred(self._sdiff, key, *keys)

    def _sdiff(self, key, *keys):
        try:
            cur_set = self.kss[key]
        except KeyError:
            # Intersection with an empty set is always an empty set
            return set()
        for k in keys:
            next_set = self.kss.get(k, set())
            cur_set = cur_set.difference(next_set)
        return cur_set

    def sdiffstore(self, dstkey, key, *keys):
        """
        @brief Same as sdiff, but result of difference is stored as dstkey.
        @param key The set from which the successive sets subtract members
        from
        @param keys The successive sets.
        @retval Deferred that fires with None
        @todo XXX what should be returned?
        """
        def _store():
            res_set = self._sdiff(key, *keys)
            self.kss[dstkey] = res_set
            return None
        return defer.maybeDeferred(_store)


    def query(self, regex):
        """
        @param regex  regular expression matching zero or more keys
        @retval Deferred for list of sets for keys matching the regex
        """
        return defer.maybeDeferred(self._query, regex)

    def _query(self, regex):
        return [re.search(regex,m).group() for m in self.kss.keys() if re.search(regex,m)]

    def remove(self, key):
        """
        @param key  an immutable key associated with a value
        @retval Deferred for success of this operation
        """
        return defer.maybeDeferred(self._remove, key)

    def _remove(self, key):
        if self.kss.has_key(key):
            del self.kss[key]




