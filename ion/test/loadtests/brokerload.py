#!/usr/bin/env python

"""
@file ion/test/loadtests/brokerload.py
@author Michael Meisinger
@brief Creates load on an AMQP broker
"""

from twisted.internet import defer

from ion.test.loadtest import LoadTest
import ion.util.procutils as pu

#import ion.util.ionlog
#log = ion.util.ionlog.getLogger(__name__)

class BrokerTest(LoadTest):

    @defer.inlineCallbacks
    def generate_load(self, *args):
        yield pu.asleep(5)
        print "LOAD"
