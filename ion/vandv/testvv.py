from twisted.internet import defer

from ion.util import procutils as pu

class TestVV(object):
    """
    Docstring
    """

    def setup(self):
        pass

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
        return

