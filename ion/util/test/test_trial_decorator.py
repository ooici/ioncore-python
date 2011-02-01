"""
@file ion/util/test/test_trial_decorator.py
@author Paul Hubbard
@author David Stuebe
@date 2/1/11
@brief Unit tests for decorator
"""

from twisted.trial import unittest
from twisted.internet import defer

import ion.util.ionlog
from ion.util.trial_decorator import itv
from ion.core import ioninit
from ion.util.trial_decorator import itv


log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

class DecoratorTest(unittest.TestCase):

    def setUp(self):
        log.debug('test setup')

    @itv(CONF)
    def test_that_passes(self):
        log.debug('This should be muffled')

    @itv(CONF)
    def test_skiptest(self):
        # Make sure we can still use skiptest
        raise unittest.SkipTest('Skipping this test')

    @itv(CONF)
    def test_that_skips(self):
        # Should never run
        self.fail('Decorator failed!')