"""
@file ion/test/test/test_trial_ccagent.py
@brief Test that the ccagent app is started for ion/trial unit tests.
@author Dorian Raymer
"""

from twisted.internet import defer

from ion.test import iontest
from ion.core.process import process

from ion.util import ionlog
log = ionlog.getLogger(__name__)

class IonTestCaseCCAgentTest(iontest.IonTestCase):

    @defer.inlineCallbacks
    def test_ccagent_starts(self):
        yield self._start_container(start_apps=['ccagent'])
        a = process.procRegistry.kvs.has_key('ccagent')
        self.failUnless(a)
        yield self._stop_container()
        b = process.procRegistry.kvs.has_key('ccagent')
        self.failIf(b)

    @defer.inlineCallbacks
    def test_ccagent_starts_again(self):
        yield self._start_container(start_apps=['ccagent'])
        a = process.procRegistry.kvs.has_key('ccagent')
        self.failUnless(a)
        yield self._stop_container()
        b = process.procRegistry.kvs.has_key('ccagent')
        self.failIf(b)
