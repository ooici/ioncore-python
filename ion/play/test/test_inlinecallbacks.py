from twisted.trial import unittest
from twisted.internet import defer, reactor
import time

class Yields(unittest.TestCase):
    runs = 10000

    def setUp(self):
        self.start_time = time.time()
        self.incrementer = 0

    def tearDown(self):
        self.end_time = time.time()
        print 'Took %.2f seconds' % (self.end_time - self.start_time)

    def _thing(self, i):
        self.incrementer += 1
        #print 'i: %d' % (self.incrementer)

    def _defer_thing(self):
        d = defer.maybeDeferred(self._thing, self.incrementer)
        return d

    @defer.inlineCallbacks
    def _yield_thing(self):
        yield self._defer_thing()

    def flat_call(self):
        [self._thing(0) for i in xrange(self.runs)]

    @defer.inlineCallbacks
    def flat_defer(self):
        ds = [self._defer_thing() for i in xrange(self.runs)]
        yield defer.DeferredList(ds)

    @defer.inlineCallbacks
    def wrap10call(self):
        def multiwrap(func, depth):
            for i in xrange(depth):
                def wrap(func):
                    def wrapper():
                        return func()
                    return wrapper
                    
                func = wrap(func)
            return func

        func = multiwrap(self._defer_thing, 10)
        ds = [func() for i in xrange(self.runs)]
        yield defer.DeferredList(ds)

    @defer.inlineCallbacks
    def wrap10yield(self):
        def multiwrap(func, depth):
            for i in xrange(depth):
                def wrap(func):
                    @defer.inlineCallbacks
                    def wrapper():
                        yield func()
                    return wrapper

                func = wrap(func)
            return func

        func = multiwrap(self._defer_thing, 10)
        ds = [func() for i in xrange(self.runs)]
        yield defer.DeferredList(ds)

    @defer.inlineCallbacks
    def wrap1yield(self):
        ds = [self._yield_thing() for i in xrange(self.runs)]
        yield defer.DeferredList(ds)
    