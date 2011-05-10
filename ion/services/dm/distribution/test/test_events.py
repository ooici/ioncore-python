#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_events.py
@author Dave Foster <dfoster@asascience.com>
@test ion.services.dm.distribution.events Test suite for event notifications
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.services.dm.distribution.publisher_subscriber import PublisherFactory, SubscriberFactory

# just a subset of the available publishers/subscribers
from ion.services.dm.distribution.events import EventPublisher, ResourceLifecycleEventPublisher, ProcessLifecycleEventPublisher, \
                                                EventSubscriber, ResourceLifecycleEventSubscriber, ProcessLifecycleEventSubscriber, \
                                                InfoLoggingEventPublisher, InfoLoggingEventSubscriber, \
                                                RESOURCE_LIFECYCLE_EVENT_ID

from ion.test.iontest import IonTestCase
from ion.core import ioninit

from ion.core.process.process import Process
from ion.core.messaging.receiver import Receiver
from ion.core.messaging import messaging
import ion.util.procutils as pu

from ion.core.exception import ReceivedError, ReceivedApplicationError, ReceivedContainerError

from ion.util.itv_decorator import itv

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

class QuickEventSubscriber(InfoLoggingEventSubscriber):
    def __init__(self, *args, **kwargs):
        self.msgs = []
        InfoLoggingEventSubscriber.__init__(self, *args, **kwargs)
                
    def ondata(self, data):
        log.debug("TestEventSubscriber received a message with name: %s",
                  data['content'].name)
        content = data['content']

        if hasattr(content, 'Repository'):
            content.Repository.persistent = True

        self.msgs.append(data)
                
class TestEventPublisher(IonTestCase):
    """
    Tests the EventPublisher and derived classes.
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self._proc = Process()
        yield self._proc.spawn()

    @defer.inlineCallbacks
    def tearDown(self):
        self._proc.terminate()
        self._proc = None
        yield self._stop_container()

    #@defer.inlineCallbacks
    def test_create(self):
        """
        Create several EventPublishers and derived versions to ensure things are setup correctly.
        """
        pub1 = EventPublisher(process=self._proc)
        self.failUnless(pub1.msg_type is None and pub1.event_id is None)
        self.failUnlessRaises(AssertionError, pub1.topic, *[None])          # can't generate topic without event id or origin
        self.failUnlessRaises(AssertionError, pub1.topic, *["orig"])        # we have no event id, so still fails

        pub2 = ResourceLifecycleEventPublisher(process=self._proc)
        self.failUnless(not pub2.msg_type is None and not pub2.event_id is None)
        self.failUnlessRaises(AssertionError, pub2.topic, *[None]) # still can't topic because we have no origin
        self.failUnlessEqual(pub2.topic("orig"), "%s.orig" % str(RESOURCE_LIFECYCLE_EVENT_ID))

        pub3 = ResourceLifecycleEventPublisher(process=self._proc, origin="orig")
        self.failUnless(hasattr(pub3, '_origin'))
        self.failUnlessEqual(pub3._origin, "orig")
        self.failUnlessEqual(pub3.topic(pub3._origin), "%s.orig" % str(RESOURCE_LIFECYCLE_EVENT_ID))

        pub4 = ProcessLifecycleEventPublisher(process=self._proc)
        self.failUnlessEqual(pub4.msg_type, pub2.msg_type)      # they share a message type
        self.failIfEqual(pub4.event_id, pub2.event_id)          # event_ids are not the same

    @defer.inlineCallbacks
    def test_create_event(self):
        """
        Creates messages with EventPublisher.create_event and examines their contents.
        """
        # Cannot create anything from base EventPublisher
        pub1 = EventPublisher(process=self._proc)
        yield pub1.initialize()
        yield pub1.activate()
        yield self.failUnlessFailure(pub1.create_event(), AssertionError)   # no msg_type defined in the base class

        # Create an empty but valid message
        pub2 = ResourceLifecycleEventPublisher(process=self._proc)
        yield pub2.initialize()
        yield pub2.activate()
        msg1 = yield pub2.create_event()     # empty, but valid!
        self.failUnlessEqual(msg1.MessageType.object_id, 2322)   # 2322 = EVENT_MESSAGE_TYPE (id)
        self.failUnlessEqual(msg1.additional_data._MessageTypeIdentifier._ID, 2323)  # 2323 = RESOURCE_LIFECYCLE_EVENT_MESSAGE_TYPE (id)

        # Test kwargs setting in both root event notification msg and additional data
        pub3 = ResourceLifecycleEventPublisher(process=self._proc)
        yield pub3.initialize()
        yield pub3.activate()
        msg2 = yield pub3.create_event(origin='orig')
        self.failUnlessEqual(msg2.origin, 'orig')

        msg3 = yield pub3.create_event()
        msg3.additional_data.state = msg3.additional_data.State.TERMINATED
        self.failUnlessEqual(msg3.additional_data.state, msg3.additional_data.State.TERMINATED)

        msg4 = yield pub3.create_event(state=msg3.additional_data.State.ERROR)
        self.failUnlessEqual(msg4.additional_data.state, msg3.additional_data.State.ERROR)
        self.failUnlessEqual(msg4.additional_data.state, msg4.additional_data.State.ERROR)

        msg5 = yield pub3.create_event(state=ResourceLifecycleEventPublisher.State.ACTIVE)
        self.failUnlessEqual(msg5.additional_data.state, msg3.additional_data.State.ACTIVE)

        self.failUnlessFailure(pub3.create_event(state='NOEXIST'), AssertionError)

        msg6 = yield pub3.create_event(state=ResourceLifecycleEventPublisher.State.READY, status=ResourceLifecycleEventPublisher.Status.NO_CACHE)
        self.failUnlessEqual(msg6.status, msg3.Status.NO_CACHE)
        self.failUnlessEqual(msg6.additional_data.state, msg3.additional_data.State.READY)

    @defer.inlineCallbacks
    def test_publish_event(self):
        """
        Test publish_event.

        The publish base class Publisher method is patched to not really do
        any sending, and is instead stored in the test class.
        """
        pub1 = ResourceLifecycleEventPublisher(process=self._proc)
        yield pub1.initialize()
        yield pub1.activate()

        # patch the publish method so that it just stores it
        def fake_publish(data, routing_key=""):
            self.lastmsg = data
            self.lastkey = routing_key

            d = defer.Deferred()
            d.callback(True)
            return d

        pub1.publish = fake_publish

        # create a message to publish
        msg = yield pub1.create_event(name="bram")
        self.failUnlessEqual(msg.name, "bram")

        # "publish" it
        yield pub1.publish_event(msg, "1234-abcdefg")
        self.failUnlessEquals(self.lastkey, "%s.1234-abcdefg" % str(RESOURCE_LIFECYCLE_EVENT_ID))
        self.failUnlessEquals(self.lastmsg.name, "bram")

        # create a publisher where we specify the origin
        pub2 = ResourceLifecycleEventPublisher(process=self._proc, origin="species")
        yield pub2.initialize()
        yield pub2.activate()
        pub2.publish = fake_publish

        # create message to publish
        msg2 = yield pub2.create_event(description="darwin")

        # publish it without specifying the origin in the publish method
        yield pub2.publish_event(msg2)
        self.failUnlessEquals(self.lastkey, "%s.species" % str(RESOURCE_LIFECYCLE_EVENT_ID))
        self.failUnlessEquals(self.lastmsg.description, "darwin")

    @defer.inlineCallbacks
    def test_create_and_publish_event(self):
        """
        Test create_and_publish_event.

        The publish base class Publisher method is patched to not really do
        any sending, and is instead stored in the test class.
        """
        pub1 = ResourceLifecycleEventPublisher(process=self._proc)
        yield pub1.initialize()
        yield pub1.activate()

        # patch the publish method so that it just stores it
        def fake_publish(data, routing_key=""):
            self.lastmsg = data
            self.lastkey = routing_key

            d = defer.Deferred()
            d.callback(True)
            return d

        pub1.publish = fake_publish

        yield pub1.create_and_publish_event(name="bram", origin="zxy-402")
        self.failUnlessEquals(self.lastkey, "%s.zxy-402" % str(RESOURCE_LIFECYCLE_EVENT_ID))
        self.failUnlessEquals(self.lastmsg.name, "bram")
        self.failUnlessEquals(self.lastmsg.origin, "zxy-402")   # both set in the msg field named "origin" and used for routing key. interesting quirk. 

    @defer.inlineCallbacks
    def test_topic_extension(self):
        """
        Create a publisher that use an extension to the topic for the purposes
        of further routing. Push a message through an event topic to make sure
        both ends can get the message at the right place.
        """
        # Setup a subscriber to an event topic
        subproc = Process()
        yield subproc.spawn()
        test_origin = "%s.%s" % ("chan1", str(subproc.id))
        testsub = QuickEventSubscriber(origin=test_origin,
                                       process=subproc)
        yield testsub.initialize()
        yield testsub.activate()
        yield pu.asleep(1.0)

        pub1 = InfoLoggingEventPublisher(process=self._proc,
                                         origin=test_origin)
        yield pub1.initialize()
        yield pub1.activate()        
        yield pu.asleep(1.0)
        
        # Toss something out with topic extension with create_and_publish
        yield pub1.create_and_publish_event(name="TestEvent")
        # Pause to make sure we catch the message
        yield pu.asleep(1.0)
        self.assertEqual(testsub.msgs[0]['content'].name, u"TestEvent")
        

class TestEventSubscriber(IonTestCase):
    """
    Tests the EventSubscriber and derived classes.
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self._proc = Process()
        yield self._proc.spawn()

    @defer.inlineCallbacks
    def tearDown(self):
        self._proc.terminate()
        self._proc = None
        yield self._stop_container()

    #@defer.inlineCallbacks
    def test_create(self):
        """
        Create several EventSubscribers and derived versions to ensure things are setup correctly.
        """
        sub1 = EventSubscriber(process=self._proc)
        self.failUnless(sub1.event_id is None)
        self.failUnlessEqual(sub1._binding_key, "*.#")

        sub2 = ResourceLifecycleEventSubscriber(process=self._proc)
        self.failUnlessEqual(sub2.event_id, RESOURCE_LIFECYCLE_EVENT_ID)
        self.failUnlessEqual(sub2._binding_key, "%s.#" % str(RESOURCE_LIFECYCLE_EVENT_ID))

        sub3 = ResourceLifecycleEventSubscriber(process=self._proc, origin="ucsd")
        self.failUnlessEqual(sub3._binding_key, "%s.ucsd" % str(RESOURCE_LIFECYCLE_EVENT_ID))

    @defer.inlineCallbacks
    def test_wildcards(self):
        """
        Test to see if wildcards match for subscribers
        """
        subproc = Process()
        yield subproc.spawn()
        test_origin = "%s.%s" % ("chan1", str(subproc.id))
        testsub = QuickEventSubscriber(origin=test_origin,
                                      process=subproc)
        yield testsub.initialize()
        yield testsub.activate()
        
        # Setup the publisher
        pub1 = InfoLoggingEventPublisher(process=self._proc,
                                         origin=test_origin)
        yield pub1.initialize()
        yield pub1.activate()
        
        # Toss something out with topic extension wildcard on the sub side
        yield pu.asleep(1.0)
        yield pub1.create_and_publish_event(name="TestEvent")
        # Pause to make sure we catch the message
        yield pu.asleep(1.0)
        self.assertEqual(testsub.msgs[0]['content'].name, u"TestEvent")
