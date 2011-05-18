try:

    from gevent import monkey; monkey.patch_all()
    import gevent
    from greenlet import greenlet
    import warnings; warnings.filterwarnings("ignore")
    from gevent import rawgreenlet
    import unittest
    import time

    raw = True
    spawn = gevent.spawn_raw if raw else gevent.spawn
    join = rawgreenlet.join if raw else lambda g: g.join()
    joinall = rawgreenlet.joinall if raw else gevent.joinall

    class Greenlets(unittest.TestCase):
        runs = 10000

        def setUp(self):
            self.start_time = time.time()
            self.incrementer = 0

        def tearDown(self):
            self.end_time = time.time()
            print 'Took %.2f seconds' % (self.end_time - self.start_time)

        def _thing(self, i=0):
            self.incrementer += 1
            #print 'i: %d' % (self.incrementer)

        def _spawn_thing(self):
            g = spawn(self._thing)
            return g

        def _join_thing(self):
            g = self._spawn_thing()
            return join(g)

        def flat_call(self):
            [self._thing(0) for i in xrange(self.runs)]

        def flat_spawn(self):
            gs = [self._spawn_thing() for i in xrange(self.runs)]
            joinall(gs)

        def wrap10call(self):
            def multiwrap(func, depth):
                for i in xrange(depth):
                    def wrap(func):
                        def wrapper():
                            return func()
                        return wrapper

                    func = wrap(func)
                return func

            func = multiwrap(self._spawn_thing, 10)
            gs = [func() for i in xrange(self.runs)]
            joinall(gs)

        def wrap10join(self):
            ''' There is no reason to ever do this in gevent '''

            def multiwrap(func, depth):
                for i in xrange(depth):
                    def wrap(func):
                        def wrapper():
                            join(spawn(func))
                        return wrapper

                    func = wrap(func)
                return func

            func = multiwrap(self._spawn_thing, 10)
            gs = [spawn(func) for i in xrange(self.runs)]
            joinall(gs)

        def wrap1join(self):
            gs = [spawn(self._join_thing) for i in xrange(self.runs)]
            joinall(gs)

except ImportError:
    pass