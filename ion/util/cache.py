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
