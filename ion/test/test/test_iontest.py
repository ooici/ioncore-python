"""
@file ion/test/test/test_iontest.py
@author Dorian Raymer
@brief Tests for stability between IonTestCase runs (because the state of the
container for any given test is coupled to the that of the previous tests!)

@note These tests should not be run with the random order option!!!
"""
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
    """This test is called ZTrivialPass to ensure that it runs AFTER
    TestSpawnProcessesFail.
    But there shouldn't be any coupling of state between unittests!, you
    say... well, unfortunately, there is here :(
    If these tests fail when run after TestSpawnProcessesFail, then there
    is a problem with self._spawn_processes handling errors.
    """

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
    """Test that an error in _spawn_processes will not cascade and cause
    false errors for subsequent tests.

    _spawn_processes will stop the container [twisted] service and then
    propagate the initial error so that setUp (or whatever method it was
    used in) fails.
    """

    @defer.inlineCallbacks
    def test_a_setUp(self):
        """
        _spawn_processes is usually called in self.setUp
        It is impossible to treat setUp like a test case, so this test case
        does what you would want to usually do in setUp.
        This test is called test_a_setUp to ensure it runs BEFORE the
        test_znoop tests.
        If the noop tests pass after test_a_setUp, then that means
        cascading errors should not be caused by the _spawn_processes
        function.
        """
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
        """dummy test to run after the container has an error during the
        setUp stage of a previous test run.
        """
        print 'NOOP 1'

    def test_znoop2(self):
        """dummy test to run after the container has an error during the
        setUp stage of a previous test run.
        """
        print 'NOOP 2'


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()



