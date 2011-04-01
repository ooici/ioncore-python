#!/usr/bin/env python

"""
@file ion/util/cache.py
@author Adam R. Smith
@brief Simple caching utilities.
"""

from time import time

class memoize(object):
    """
    Memoize with timeout.
    This has been optimized repeatedly to squeeze out extra performance.
    
    http://code.activestate.com/recipes/325905/ (r5)
    Modified by Adam R. Smith
    """
    
    _caches = {}
    _timeouts = {}

    def __init__(self, timeout=0):
        self.timeout = timeout

    def collect(self):
        """Clear cache of results which have timed out"""
        for func in self._caches:
            cache = {}
            for key in self._caches[func]:
                if (self._timeouts[func] <= 0) or ((time() - self._caches[func][key][1]) < self._timeouts[func]):
                    cache[key] = self._caches[func][key]
            self._caches[func] = cache

    def __call__(self, f):
        cache = self.cache = self._caches[f] = {}
        timeout = self._timeouts[f] = self.timeout

        def func(*args, **kwargs):
            key = tuple(args)
            if len(kwargs):
                kw = kwargs.items()
                kw.sort()
                key += tuple(kw)
                
            cached = (key in cache)
            if cached:
                v = cache[key]
                if (timeout > 0) and ((time() - v[1]) > timeout):
                    cached = False
            if not cached:
                ts = time() if timeout else 0
                v = cache[key] = (f(*args, **kwargs), ts)
            return v[0]
        #func.func_name = f.func_name
        func.__doc__ = f.__doc__

        return func


class LRUDict(object):
    """
    Amortized O(1) LRU cache with a dict-like interface. Based on http://code.activestate.com/recipes/252524/ (r3)
    Copyright 2003 Josiah Carlson.
    Modified by Adam R. Smith to support sizes and to be more dict-like.
    Licensed under the PSF License: http://docs.python.org/license.html
    """

    class Node(object):
        __slots__ = ['prev', 'next', 'me', 'size']
        def __init__(self, prev, me, size=1):
            self.prev = prev
            self.me = me
            self.next = None
            self.size = size


    def __init__(self, limit, pairs=None, use_size=False):
        """ limit is either an integer item count or a size in bytes. """

        self.limit = max(limit, 1)
        self.d = {}
        self.first = None
        self.last = None
        self.use_size = use_size
        self.total_size = 0

        if pairs is None: pairs = []
        for key, value in pairs:
            self[key] = value

    def __contains__(self, key):
        return key in self.d

    def has_key(self, key):
        return key in self.d

    def __getitem__(self, key):
        a = self.d[key].me
        self[a[0]] = a[1]
        return a[1]

    def __setitem__(self, key, val):
        if key in self.d:
            del self[key]

        size = 1
        if self.use_size and hasattr(val, '__sizeof__'):
            size = val.__sizeof__()
        self.total_size += size

        nobj = LRUDict.Node(self.last, (key, val), size)
        if self.first is None:
            self.first = nobj
        if self.last:
            self.last.next = nobj
        self.last = nobj
        self.d[key] = nobj

        self.purge()

    def purge(self):
        while self.total_size > self.limit:
            if self.first == self.last:
                self.first = None
                self.last = None
                self.total_size = 0
                return

            a = self.first
            self.total_size -= a.size
            a.next.prev = None
            self.first = a.next
            a.next = None
            del self.d[a.me[0]]
            del a

    def __delitem__(self, key):
        nobj = self.d[key]
        self.total_size -= nobj.size

        if nobj.prev:
            nobj.prev.next = nobj.next
        else:
            self.first = nobj.next
        if nobj.next:
            nobj.next.prev = nobj.prev
        else:
            self.last = nobj.prev
        del self.d[key]

    def __iter__(self):
        cur = self.first
        while cur != None:
            cur2 = cur.next
            yield cur.me[1]
            cur = cur2

    def iteritems(self):
        cur = self.first
        while cur != None:
            cur2 = cur.next
            yield cur.me
            cur = cur2

    def iterkeys(self):
        return iter(self.d)

    def itervalues(self):
        for i,j in self.iteritems():
            yield j

    def keys(self):
        return self.d.keys()

    def touch(self, key):
        """ Recalculate the size of the object at the given key, and update its access time. """
        val = self[key]
        if self.use_size and hasattr(val, '__sizeof__'):
            old_size = val.size
            val.size = val.__sizeof__()
            self.total_size += val.size - old_size

        self.purge()
        return val

    def get(self, key, default=None):
        if key in self.d:
            return self[key]
        return default

    def update(self, d):
        for k,v in d.iteritems():
            self[k] = v

    def clear(self):
        self.d.clear()
        self.total_size = 0
        self.first = None
        self.last = None

if __name__ == '__main__':
    def main():
        class ObjectWithSize(object):
            def __init__(self, size):
                self.size = size
            def __sizeof__(self):
                return self.size

        lru = LRUDict(3)
        lru['a'] = 1
        lru['b'] = 2
        lru['c'] = 3
        lru['a'] = 1
        lru['d'] = 4
        print 'Should be: a, c, d', lru.keys()

        lru = LRUDict(limit=100, use_size=True)
        lru['a'] = ObjectWithSize(25)
        lru['b'] = ObjectWithSize(50)
        lru['c'] = ObjectWithSize(25)
        lru['d'] = ObjectWithSize(1)
        print 'Should be: c, b, d', lru.keys()

        lru.touch('b')
        lru.touch('c')
        lru['a'] = ObjectWithSize(25)
        print 'Should be: a, c, b', lru.keys()

        lru.update({'e': ObjectWithSize(1), 'f': ObjectWithSize(2), 'g': ObjectWithSize(20)})

        for k,v in lru.iteritems():
            print '%s: %s' % (k, str(v))

        lru.clear()
        print 'Should be empty: ', lru.keys()

        print 'Should be false: ', lru.has_key('monkey')

    main()
