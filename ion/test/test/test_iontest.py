
from ion.test import iontest

class TestFailStartContainer(iontest.IonTestCase):
    """wouldn't it be nice if _start_container was called automatically?
    then we could privately handle the case that it fails.
    -- Actually, it is spawn processes that can fail.
    -- We just need to make sure stopContainer gets called.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.fail()

    def tearDown(self):
        yield self._stop_container()
