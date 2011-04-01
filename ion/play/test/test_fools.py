#!/usr/bin/env python

"""
@author David Stuebe
@author David Foster
"""
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest

from ion.util import procutils as pu


class TestFool(unittest.TestCase):

    @defer.inlineCallbacks
    def test_april_fools(self):

        msg="""
    ___               _ __   ______             __      __
   /   |  ____  _____(_) /  / ____/____  ____  / /_____/ /
  / /| | / __ \/ ___/ / /  / /_   / __ \/ __ \/ // ___/ /
 / ___ |/ /_/ / /  / / /  / __/  / /_/ / /_/ / /(__  )_/
/_/  |_/ .___/_/  /_/_/  /_/     \____/\____/_//____(_)
      /_/
"""
        print msg
        yield pu.asleep(5)
        