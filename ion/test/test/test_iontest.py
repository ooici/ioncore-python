
from twisted.internet import defer

from ion.test import iontest

class TestFailStartContainer:#(iontest.IonTestCase):
    """wouldn't it be nice if _start_container was called automatically?
    then we could privately handle the case that it fails.
    -- Actually, it is spawn processes that can fail.
    -- We just need to make sure stopContainer gets called.
    """

    @defer.inlineCallbacks
    def setUp(self):
        print 'setUp!!'
        print self.twisted_container_service
        yield self._start_container()
        try:
            self.fail()
        except Exception, ex:
            yield self._stop_container()
            raise ex

    def test_noop1(self):
        print 'NOOP 1'

    def test_noop2(self):
        print 'NOOP 2'

    @defer.inlineCallbacks
    def tearDown(self):
        print 'tearDown$$$'
        yield self._stop_container()


class TestZTrivialPass(iontest.IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        print 'setUp!!'
        yield self._start_container()

    def test_noop1(self):
        print 'NOOP 1'

    def test_noop2(self):
        print 'NOOP 2'

    @defer.inlineCallbacks
    def test_stop_container(self):
        yield self._stop_container()

    def test_zlastnoop3(self):
        print 'NOOP 3'

    @defer.inlineCallbacks
    def tearDown(self):
        print 'tearDown$$$'
        yield self._stop_container()


class TestSpawnProcessesFail(iontest.IonTestCase):

    @defer.inlineCallbacks
    def test_a_setUp(self):
        services = [
                {
                    'name':'test',
                    'module':'scion.core.cc.cc_agent',
                    'class':'Bad',
                    }
                ]
        yield self._start_container()
        yield self.failUnlessFailure(self._spawn_processes(services), ImportError)

    def test_znoop1(self):
        print 'NOOP 1'

    def test_znoop2(self):
        print 'NOOP 2'


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()



