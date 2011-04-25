#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_publisher_subscriber.py
@author Dave Foster <dfoster@asascience.com>
@test ion.services.dm.distribution.publisher_susbcriber Test suite for revised pubsub code
"""
from twisted.trial.unittest import SkipTest
import ion.util.ionlog
from twisted.internet import defer

from ion.services.dm.distribution.publisher_subscriber import Publisher, PublisherFactory, Subscriber, SubscriberFactory
from ion.util.state_object import BasicStates
#from ion.services.dm.distribution.pubsub_service import PubSubClient, REQUEST_TYPE
#from ion.services.dm.distribution.publisher_subscriber import Subscriber
from ion.test.iontest import IonTestCase
from ion.core import ioninit

from ion.core.process.process import Process
from ion.core.messaging.receiver import Receiver
from ion.core.messaging import messaging
import ion.util.procutils as pu

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

class TestPublisher(IonTestCase):
    """
    """
    @defer.inlineCallbacks
    def setUp(self):
        services = [
            {
                'name':'pubsub_service',
                'module':'ion.services.dm.distribution.pubsub_service',
                'class':'PubSubService'
            },
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                    'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry.resource_registry',
                'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}},
            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },
            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },

            ]
        yield self._start_container()
        self.sup = yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_publisher_create(self):
        """
        Create/activate a publisher without factories.  Make sure all required components are present.
        """

        # Publishers need to be owned by a process
        proc = Process()
        yield proc.spawn()

        # "Publisher" is a callable, remember
        self.failUnlessRaises(AssertionError, Publisher)                                    # needs xp_name, routing_key, process

        # test all possible combinations of arguments that should fail, first singles, then doubles
        args = [('xp_name', 'magnet_topic'),
                ('routing_key', 'arf.test'),
                ('process', proc)]

        self.failUnlessRaises(AssertionError, Publisher, **dict([args[0]])) # xp_name
        self.failUnlessRaises(AssertionError, Publisher, **dict([args[1]])) # routing_key
        self.failUnlessRaises(AssertionError, Publisher, **dict([args[2]])) # process

        self.failUnlessRaises(AssertionError, Publisher, **dict([args[0]] + [args[1]])) # xp_name + routing_key
        self.failUnlessRaises(AssertionError, Publisher, **dict([args[0]] + [args[2]])) # xp_name + proc
        self.failUnlessRaises(AssertionError, Publisher, **dict([args[1]] + [args[2]])) # routing_key + proc

        # now construct one with everything correct
        pub1 = Publisher(**dict(args))      # all requirements satisfied
        self.failUnlessIsInstance(pub1, Publisher)

        # now attach it to the process
        yield proc.register_life_cycle_object(pub1)

        self.failUnless(pub1._get_state() == BasicStates.S_ACTIVE)      # register_life_cycle_object will move the publisher to match the proc's state

    #noinspection PyUnreachableCode
    @defer.inlineCallbacks
    def test_psc_plus_factory(self):
        raise SkipTest('Broker bug, no current workaround')
        # a Publisher is attached to a process
        proc = Process()
        yield proc.spawn()

        fact = PublisherFactory(xp_name='science_data', process=proc)
        yield fact.build(routing_key='fubar')

    @defer.inlineCallbacks
    def test_publisher_factory_create(self):

        # a Publisher is attached to a process
        proc = Process()
        yield proc.spawn()

        # factory without any args doesn't fill in xp_name
        fact = PublisherFactory()

        # we didn't specify xp_name in factory creation nor here, so it will error
        yield self.failUnlessFailure(fact.build(routing_key='arf.test'), AssertionError)
        yield self.failUnlessFailure(fact.build(routing_key='arf.test', process=proc), AssertionError)

        # specify all
        pub = yield fact.build(routing_key="arf.test", xp_name="magnet.topic", process=proc)

        # we should get an active Publisher back here
        self.failUnlessIsInstance(pub, Publisher)
        self.failUnless(pub._get_state() == BasicStates.S_ACTIVE)
        self.failUnless(pub._process == proc)

        # now lets make a factory where we can specify the xp_name as a default
        fact2 = PublisherFactory(xp_name="magnet.topic")

        pub2 = yield fact2.build(routing_key="arf.test", process=proc)

        self.failUnlessIsInstance(pub2, Publisher)
        self.failUnless(pub2._process == proc)
        self.failUnless(pub2._get_state() == BasicStates.S_ACTIVE)
        self.failUnless(pub2._recv.publisher_config.has_key("exchange") and pub2._recv.publisher_config['exchange'] == "magnet.topic")

        # use the same factory to override the default xp_name
        pub3 = yield fact2.build(routing_key="arf.test", xp_name="afakeexchange", process=proc)

        self.failUnlessIsInstance(pub3, Publisher)
        self.failUnless(pub3._process == proc)
        self.failUnless(pub3._get_state() == BasicStates.S_ACTIVE)
        self.failUnless(pub3._recv.publisher_config.has_key("exchange") and pub3._recv.publisher_config['exchange'] == "afakeexchange")

    class TestPubRecv(Receiver):
        """
        A Test Receiver to listen to publishings.

        TODO: move this into base receiver?
        """
        def __init__(self, *args, **kwargs):
            binding_key = kwargs.pop('binding_key', None)
            self.msgs = []
            Receiver.__init__(self, *args, **kwargs)
            if binding_key is None:
               binding_key = self.xname

            self.binding_key = binding_key

        @defer.inlineCallbacks
        def on_initialize(self, *args, **kwargs):
            name_config = messaging.worker(self.xname)
            name_config.update({'name_type':'worker', 'binding_key':self.binding_key, 'routing_key':self.binding_key})

            yield self._init_receiver(name_config, store_config=True)

            self.add_handler(self.blab)

        def blab(self, content, msg):
            msg.ack()
            self.msgs.append(content['content'])

    @defer.inlineCallbacks
    def test_publish(self):

        # a publisher needs a Process
        proc = Process()
        yield proc.spawn()

        fact = PublisherFactory(xp_name="magnet.topic", process=proc)

        pub = yield fact.build(routing_key="arf.test")

        testsub = self.TestPubRecv(name="arf.test", binding_key="arf.test")
        yield testsub.attach()

        # send a message
        yield pub.publish("this is a sample, beats are fresh")

        # sleep just a bit to let message go through
        yield pu.asleep(1.0)

        # we should see it now in the testsub's collection
        self.failUnlessEqual(len(testsub.msgs), 1)
        self.failUnlessEqual(testsub.msgs[0], "this is a sample, beats are fresh")

# #####################################################################################

class TestSubscriber(IonTestCase):
    """
    """
    @defer.inlineCallbacks
    def setUp(self):
        services = [
            {
                'name':'pubsub_service',
                'module':'ion.services.dm.distribution.pubsub_service',
                'class':'PubSubService'
            },
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                    'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry.resource_registry',
                'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}},
            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },
            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },

            ]
        yield self._start_container()
        self.sup = yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    #@defer.inlineCallbacks
    def test_subscriber_create(self):
        """
        Create/activate a subscriber without factories.  Make sure all required components are present.
        """

        # subscriber needs a process
        proc = Process()
        yield proc.spawn()

        self.failUnlessRaises(AssertionError, Subscriber)   # needs xp_name

        # test all combinations of arguments (not really necessary as Subscriber currently only has two required args,
        # but this keeps it in line with the Publisher test above and makes it simple to add another range of tests
        # later)
        args = [('xp_name','magnet.topic'),
                ('process',proc)]

        self.failUnlessRaises(AssertionError, Subscriber, **dict([args[0]]))    # xp_name
        self.failUnlessRaises(AssertionError, Subscriber, **dict([args[1]]))    # process

        # all required arguments
        sub = Subscriber(**dict(args))
        self.failUnlessIsInstance(sub, Subscriber)

    @defer.inlineCallbacks
    def test_subscriber_factory_create(self):
        # subscriber needs a process
        proc = Process()
        yield proc.spawn()

        sf = SubscriberFactory()

        # needs an xp_name and a process, so this build should fail
        self.failUnlessFailure(sf.build(), AssertionError)

        # test all combinations of arguments (not really necessary as Subscriber currently only has two required args,
        # but this keeps it in line with the Publisher test above and makes it simple to add another range of tests
        # later)
        args = [('xp_name','magnet.topic'),
                ('process',proc)]

        log.debug('This should fail')
        self.failUnlessFailure(sf.build(**dict([args[0]])), AssertionError)    # xp_name
        log.debug('This should fail too')
        self.failUnlessFailure(sf.build(**dict([args[1]])), AssertionError)    # process

        log.debug('this one should work')
        sub = yield sf.build(**dict(args))
        self.failUnlessIsInstance(sub, Subscriber)
        self.failUnless(sub._get_state() == BasicStates.S_ACTIVE)
        self.failUnless(sub._process == proc)

        # now lets make a factory where we can specify the xp_name and process as defaults
        sf2 = SubscriberFactory(**dict(args))

        log.debug('this one should also work')
        sub2 = yield sf2.build()

        self.failUnlessIsInstance(sub2, Subscriber)
        self.failUnless(sub2._get_state() == BasicStates.S_ACTIVE)
        self.failUnless(sub2._recv.consumer_config.has_key("exchange") and sub2._recv.consumer_config['exchange'] == "magnet.topic")
        self.failUnless(sub2._process == proc)

        # use the same factory to override the default xp_name
        log.debug('failure is not an option')
        sub3 = yield sf2.build(xp_name="afakeexchange")

        self.failUnlessIsInstance(sub3, Subscriber)
        self.failUnless(sub3._get_state() == BasicStates.S_ACTIVE)
        self.failUnless(sub3._recv.consumer_config.has_key("exchange") and sub3._recv.consumer_config['exchange'] == "afakeexchange")
        self.failUnless(sub3._process == proc)

    @defer.inlineCallbacks
    def test_subscriber_queue_bindings(self):
        """
        Tests the various combinations of queue and binding specifications.

        TODO: not good ways of testing these, need broker interaction to really tell
        """
        # subscriber needs a Process
        proc = Process()
        yield proc.spawn()

        sf = SubscriberFactory(xp_name="magnet.topic", process=proc)
        sub = yield sf.build()
        self.failUnlessIsInstance(sub, Subscriber)

        # hmm.. activating this subscriber (via spawn) would create an anonymous queue with no binding to it, how to test this?
        # let's just see if it sets sub._recv.backend.queue to an anonymous queue name
        self.failUnless(hasattr(sub._recv.consumer, 'queue'))

        sub = yield sf.build(binding_key="arf.test")
        self.failUnless(hasattr(sub._recv.consumer, 'queue'))
        #self.failUnless(sub._recv.consumer         # TODO: TEST BINDING?

        # when you specify a queue, it doesn't get saved as an attr
        sub = yield sf.build(binding_key="arf.test", queue_name="arfbark")
        #self.failIf(hasattr(sub._recv.consumer, 'queue'))
        self.failUnless(sub._recv.consumer.queue == "arfbark")

        # it's okay to specify a queue with no binding
        sub = yield sf.build(queue_name="ardtest")
        self.failUnless(sub._recv.consumer.queue == "ardtest")

# #####################################################################################

class TestPublisherAndSubscriber(IonTestCase):
    """
    """
    @defer.inlineCallbacks
    def setUp(self):
        services = [
            {
                'name':'pubsub_service',
                'module':'ion.services.dm.distribution.pubsub_service',
                'class':'PubSubService'
            },
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                    'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry.resource_registry',
                'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}},
            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },
            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },

            ]
        yield self._start_container()
        self.sup = yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_publish_subscribe(self):
        """
        """

        proc = Process()
        yield proc.spawn()

        pf = PublisherFactory(xp_name="magnet.topic", process=proc)
        sf = SubscriberFactory(xp_name="magnet.topic", process=proc)

        msgs = []
        def handle_msg(content):
            msgs.append(content['content'])

        pub = yield pf.build(routing_key='arf_test')
        yield sf.build(binding_key='arf_test', handler=handle_msg)

        yield pub.publish('get stuck in')
        yield pu.asleep(1.0)
        self.failUnless(len(msgs)==1 and msgs[0] == "get stuck in")

        self.failUnlessRaises(AssertionError, Subscriber)   # needs xp_name



