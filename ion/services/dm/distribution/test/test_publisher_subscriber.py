#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_publisher_subscriber.py
@author Dave Foster <dfoster@asascience.com>
@test ion.services.dm.distribution.publisher_susbcriber Test suite for revised pubsub code
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.services.dm.distribution.publisher_subscriber import Publisher, PublisherFactory
from ion.util.state_object import BasicStates
#from ion.services.dm.distribution.pubsub_service import PubSubClient, REQUEST_TYPE
#from ion.services.dm.distribution.publisher_subscriber import Subscriber
from ion.test.iontest import IonTestCase
from twisted.trial import unittest
from ion.util.procutils import asleep
from ion.core import ioninit

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient

from ion.core.exception import ReceivedError, ReceivedApplicationError, ReceivedContainerError

from ion.util.itv_decorator import itv

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

class TestPublisher(IonTestCase):
    """
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_publisher_create(self):
        """
        Create/activate a publisher without factories.  Make sure all required components are present.
        """

        # "Publisher" is a callable, remember
        self.failUnlessRaises(AssertionError, Publisher)                                    # needs both xp_name and routing_key
        self.failUnlessRaises(AssertionError, Publisher, **{'xp_name':'magnet.topic'})      # needs routing_key
        self.failUnlessRaises(AssertionError, Publisher, **{'routing_key':'arf.test'})      # needs xp_name

        pub1 = Publisher(xp_name="magnet.topic", routing_key="arf.test")                    # all requirements satisfied
        self.failUnlessIsInstance(pub1, Publisher)

        # now attach it
        yield pub1.initialize()
        yield pub1.activate()

        self.failUnless(pub1._get_state() == BasicStates.S_ACTIVE)

    @defer.inlineCallbacks
    def test_publisher_factory_create(self):

        # factory without any args doesn't fill in xp_name
        fact = PublisherFactory()

        # we didn't specify xp_name in factory creation nor here, so it will error
        self.failUnlessFailure(fact.build(routing_key='arf.test'), AssertionError)

        # specify both
        pub = yield fact.build(routing_key="arf.test", xp_name="magnet.topic")

        # we should get an active Publisher back here
        self.failUnlessIsInstance(pub, Publisher)
        self.failUnless(pub._get_state() == BasicStates.S_ACTIVE)

        # now lets make a factory where we can specify the xp_name as a default
        fact2 = PublisherFactory(xp_name="magnet.topic")

        pub2 = yield fact2.build(routing_key="arf.test")

        self.failUnlessIsInstance(pub2, Publisher)
        self.failUnless(pub2._get_state() == BasicStates.S_ACTIVE)
        self.failUnless(pub2._recv.publisher_config.has_key("exchange") and pub2._recv.publisher_config['exchange'] == "magnet.topic")

        # use the same factory to override the default xp_name
        pub3 = yield fact2.build(routing_key="arf.test", xp_name="afakeexchange")

        self.failUnlessIsInstance(pub3, Publisher)
        self.failUnless(pub3._get_state() == BasicStates.S_ACTIVE)
        self.failUnless(pub3._recv.publisher_config.has_key("exchange") and pub3._recv.publisher_config['exchange'] == "afakeexchange")


