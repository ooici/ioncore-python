#!/usr/bin/env python
from ion.util import procutils

import ion.util.ionlog
import ion.util.procutils

log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.test.iontest import IonTestCase



class IdleClientTest(IonTestCase):
    """
    Testing example hello service business logic
    """

    @defer.inlineCallbacks
    def setUp(self):
        
        # Starting the container is required! That way you can use the test supervisor process
        yield self._start_container()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

        
    @defer.inlineCallbacks
    def xtest_hello_accept(self):
        while True:
            yield procutils.asleep(60)
