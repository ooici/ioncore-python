from twisted.internet import defer

from ion.util import procutils as pu
from ion.vandv.vandvbase import VVBase

class TestVV(VVBase):
    """
    This is where I describe what this test does.
    """

    @defer.inlineCallbacks
    def setup(self):
        yield self._start_itv(files=["itv_start_files/boot_level_4.itv"])

    def s1_broheim(self):
        """
        1. Setup a broheim shindig
        """

        print "3"
        return

    @defer.inlineCallbacks
    def s2_grindig(self):
        """
        2. A grindig is formed
        """

        print "german burger"

        yield pu.asleep(3)
        defer.returnValue(None)

