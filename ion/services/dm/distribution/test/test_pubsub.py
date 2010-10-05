#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_pubsub.py
@author Michael Meisinger
@author David Stuebe
@author Matt Rodriguez
@brief test service for registering topics for data publication & subscription
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
#import time
from twisted.internet import defer



from ion.core import bootstrap
from ion.core.exception import ReceivedError
from ion.services.dm.distribution.pubsub_service import DataPubsubClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import PublisherResource,\
    PubSubTopicResource, SubscriptionResource,DataMessageObject



from ion.services.dm.distribution import base_consumer


from ion.services.dm.util import dap_tools

class PubSubEndToEndTest(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'}
            ]

        self.sup = yield self._spawn_processes(services)
        self.pubsub_client = DataPubsubClient(self.sup)

        child_id = yield self.sup.get_child_id('pubsub_service')
        log.debug('PubSub Test Service ID:' + str(child_id))
        self.pubsub = self._get_procinstance(child_id)


        self.topic1 = PubSubTopicResource.create('Test1 Topic',"Grids")
        self.topic2 = PubSubTopicResource.create('Test2 Topic',"Points")

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.pubsub.reg.clear_registry()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_publisher(self):
        """
        Publish to a topic that has been registered with a publisher.
        """
        self.topic1 = yield self.pubsub_client.define_topic(self.topic1)
        self.pub1 = PublisherResource.create("Grids", self.sup, [self.topic1], "content")
        yield self.pubsub_client.define_publisher(self.pub1)
        res = yield self.pubsub_client.publish(self.sup, self.topic1, "Gridded data")
        self.assertEqual(res, 'sent')

    @defer.inlineCallbacks
    def test_publisher_fail(self):
        """
        Try to publish to a topic that has not been registered to a publisher.
        @note: This test registers topic1 to a publisher, then tries to publish
        to topic2.
        """
        self.topic2 = yield self.pubsub_client.define_topic(self.topic2)
        self.pub1 = PublisherResource.create("Grids", self.sup, [self.topic1], "content")
        yield self.pubsub_client.define_publisher(self.pub1)
        try:
            yield self.pubsub_client.publish(self.sup, self.topic2, "Point data")
            self.fail("ReceivedError expected")
        except ReceivedError, re:
            pass

class PubSubServiceMethodTest(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'}
            ]

        self.sup = yield self._spawn_processes(services)

        # This is only allowed in a test case - that way we can directly test the service methods!
        child_id = yield self.sup.get_child_id('pubsub_service')
        log.debug('PubSub Test Service ID:' + str(child_id))
        self.pubsub = self._get_procinstance(child_id)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.pubsub.reg.clear_registry()
        yield self._stop_container()



    @defer.inlineCallbacks
    def test_create_topic(self):
        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")

        # Make sure the topic starts out with blank stuff here...
        self.assertEqual(topic.RegistryBranch,'master')
        self.assertEqual(topic.RegistryIdentity,'')
        self.assertEqual(topic.queue.type,'')

        # Use the service to create a queue and register the topic
        topic = yield self.pubsub.create_and_register_topic(topic)

        # Make sure the queue properties were set.
        self.assertEqual(topic.queue.type,'fanout')

        #Spawn an baseconsumer and make sure a message is received on the new queue
        pd1={'name':'consumer_number_1',
                 'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'procclass':'LoggingConsumer',
                 'spawnargs':{'attach':[topic.queue.name]}}
        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)

        msg=DataMessageObject()
        self.sup.send(topic.queue.name,'data',msg.encode())

        yield pu.asleep(1)

        dc1 = self._get_procinstance(child1_id)
        #print 'DC1',dc1.receive_cnt
        rec = dc1.receive_cnt[topic.queue.name]
        self.assertEqual(rec,1)

    @defer.inlineCallbacks
    def test_create_consumer_args1(self):
        '''
        @brief Test business logic to create consumer args from a workflow
        Make sure teh basics of the workflow processing work to create consumer
        args.
        '''

        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")
        # Use the service to create a queue and register the topic
        topic = yield self.pubsub.create_and_register_topic(topic)


        # A condition to search for the topic
        t_search = PubSubTopicResource.create('Davids Topic','')

        subscription = SubscriptionResource()
        subscription.topic1 = t_search

        subscription.workflow = {'consumer1':{'module':'path.to.module','consumerclass':'<ConsumerClassName>',\
            'attach':'topic1',\
            'process parameters':{'param1':'my parameter'}}}

        subscription = yield self.pubsub.create_consumer_args(subscription)

        log.debug('Defined subscription consumers: '+str(subscription.consumer_args))
        self.assertEqual({'consumer1':
            {'procclass': '<ConsumerClassName>',
             'name': 'consumer1',
             'module': 'path.to.module',
             'spawnargs': {
                'attach': [topic.queue.name],
                'delivery interval': None,
                'delivery queues': {},
                'process parameters': {'param1': 'my parameter'}}}}, subscription.consumer_args)

    @defer.inlineCallbacks
    def test_create_consumer_args2(self):
        '''
        @brief Test business logic to create consumer args from a workflow
        In this test there is a second consumer a real workflow - test shows that
        they correctly delivery/attach to the same queue
        '''

        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")
        # Use the service to create a queue and register the topic
        topic = yield self.pubsub.create_and_register_topic(topic)


        # A condition to search for the topic
        t_search = PubSubTopicResource.create('Davids Topic','')

        subscription = SubscriptionResource()
        subscription.topic1 = t_search

        subscription.workflow = {
            'consumer1':{'module':'path.to.module','consumerclass':'<ConsumerClassName>',\
            'attach':'topic1',\
            'process parameters':{'param1':'my parameter1'}},

            'consumer2':{'module':'path.to.module2','consumerclass':'<ConsumerClassName2>',\
            'attach':[['consumer1','deliver_to']],\
            'Process Parameters':{'param2':'my parameter2'}}
                }

        subscription = yield self.pubsub.create_consumer_args(subscription)

        log.debug('Defined subscription consumers: '+str(subscription.consumer_args))

        consume2_attach = subscription.consumer_args['consumer2']['spawnargs']['attach']
        self.assertEqual({'module': 'path.to.module',
               'name': 'consumer1',
               'procclass': '<ConsumerClassName>',
               'spawnargs': {'attach': [topic.queue.name],
                             'delivery interval': None,
                             'delivery queues': {'deliver_to': consume2_attach[0]},
                             'process parameters': {'param1': 'my parameter1'}}},
            subscription.consumer_args['consumer1'])


    @defer.inlineCallbacks
    def test_create_consumer_args3(self):
        '''
        @brief Test business logic to create consumer args from a workflow
        In this test two consumers attach to the same queue created by the workflow
        '''

        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")
        # Use the service to create a queue and register the topic
        topic = yield self.pubsub.create_and_register_topic(topic)


        # A condition to search for the topic
        t_search = PubSubTopicResource.create('Davids Topic','')

        subscription = SubscriptionResource()
        subscription.topic1 = t_search

        subscription.workflow = {
            'consumer1':{'module':'path.to.module','consumerclass':'<ConsumerClassName>',\
            'attach':'topic1',\
            'process parameters':{'param1':'my parameter1'}},

            'consumer2':{'module':'path.to.module2','consumerclass':'<ConsumerClassName2>',\
            'attach':[['consumer1','deliver_to']],\
            'Process Parameters':{'param2':'my parameter2'}},

            'consumer3':{'module':'path.to.module3','consumerclass':'<ConsumerClassName3>',\
            'attach':[['consumer1','deliver_to'],'topic1']}
                }

        subscription = yield self.pubsub.create_consumer_args(subscription)

        log.debug('Defined subscription consumers: '+str(subscription.consumer_args))

        consume2_attach = subscription.consumer_args['consumer2']['spawnargs']['attach']
        self.assertEqual({'module': 'path.to.module',
               'name': 'consumer1',
               'procclass': '<ConsumerClassName>',
               'spawnargs': {'attach': [topic.queue.name],
                             'delivery interval': None,
                             'delivery queues': {'deliver_to': consume2_attach[0]},
                             'process parameters': {'param1': 'my parameter1'}}},
            subscription.consumer_args['consumer1'])

        self.assertEqual({'procclass': '<ConsumerClassName3>',
         'name': 'consumer3',
         'module': 'path.to.module3',
         'spawnargs': {
            'attach': [consume2_attach[0], topic.queue.name],
            'delivery interval': None,
            'delivery queues': {},
            'process parameters': {}}},
            subscription.consumer_args['consumer3'])



    @defer.inlineCallbacks
    def test_create_subscription1(self):
        '''
        @brief Create a subscription!
        '''

        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")
        # Use the service to create a queue and register the topic
        topic = yield self.pubsub.create_and_register_topic(topic)


        # A condition to search for the topic
        t_search = PubSubTopicResource.create('Davids Topic','')

        subscription = SubscriptionResource()
        subscription.topic1 = t_search

        subscription.workflow = {
            'consumer1':
                {'module':'ion.services.dm.distribution.consumers.forwarding_consumer',
                 'consumerclass':'ForwardingConsumer',\
                 'attach':'topic1'}}

        subscription = yield self.pubsub.create_subscription(subscription)

        log.info('Defined subscription: '+str(subscription))

        msg=DataMessageObject()
        self.sup.send(topic.queue.name,'data',msg.encode())

        # Wait for message to be received
        yield pu.asleep(1)
        child_id = self.pubsub.get_child_id('consumer1')
        dc1 = self._get_procinstance(child_id)
        rec = dc1.receive_cnt[topic.queue.name]
        self.assertEqual(rec,1)

    @defer.inlineCallbacks
    def test_create_subscription2(self):
        '''
        @brief Create a subscription!
        '''
        # Create and Register a topic
        topic1 = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")
        # Use the service to create a queue and register the topic
        topic1 = yield self.pubsub.create_and_register_topic(topic1)

        topic2 = PubSubTopicResource.create('Johns Topic',"oceans, mbari, working really hard")
        # Use the service to create a queue and register the topic
        topic2 = yield self.pubsub.create_and_register_topic(topic2)

        subscription = SubscriptionResource()
        subscription.topic1 = PubSubTopicResource.create('Davids Topic','')
        subscription.topic2 = PubSubTopicResource.create('','oceans')

        subscription.workflow = {
            'consumer1':
                {'module':'ion.services.dm.distribution.consumers.forwarding_consumer',
                 'consumerclass':'ForwardingConsumer',\
                 'attach':'topic1'},
            'consumer2':
                {'module':'ion.services.dm.distribution.consumers.forwarding_consumer',
                 'consumerclass':'ForwardingConsumer',\
                 'attach':[['consumer1','queues']]}
                }

        subscription = yield self.pubsub.create_subscription(subscription)

        log.info('Defined subscription: '+str(subscription))

        msg=DataMessageObject()
        self.sup.send(topic1.queue.name,'data',msg.encode())

        # Wait for message to be received
        yield pu.asleep(1)
        child1_id = self.pubsub.get_child_id('consumer1')
        dc1 = self._get_procinstance(child1_id)
        rec = dc1.receive_cnt[topic1.queue.name]
        self.assertEqual(rec,1)

        child2_id = self.pubsub.get_child_id('consumer2')
        dc2 = self._get_procinstance(child2_id)

        q = subscription.consumer_args['consumer2']['spawnargs']['attach']
        rec = dc2.receive_cnt[q[0]]
        self.assertEqual(rec,1)





class PubSubTest(IonTestCase):
    """
    Testing PubSub service methods to define topics, define publishers, define
    subscriptions and publish data.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'}
            ]

        self.sup = yield self._spawn_processes(services)


    @defer.inlineCallbacks
    def tearDown(self):

        log.info('Tearing Down PubSub Test')
        # Clear the registry on the way out!
        child_id = yield self.sup.get_child_id('pubsub_service')
        log.debug('PubSub Test Service ID:' + str(child_id))
        # This is only allowed in a test case - that way we can directly use the service methods!
        pubsub = self._get_procinstance(child_id)
        pubsub.reg.clear_registry()

        yield pu.asleep(1)

        yield self._stop_container()

    def test_setup_teardown(self):
        pass


    @defer.inlineCallbacks
    def test_pubsub(self):

        dpsc = DataPubsubClient(self.sup)

        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")
        topic = yield dpsc.define_topic(topic)
        log.info('Defined Topic: '+str(topic))

        #Create and register self.sup as a publisher
        publisher = PublisherResource.create('Test Publisher', self.sup, topic, 'DataObject')
        publisher = yield dpsc.define_publisher(publisher)

        log.info('Defined Publisher: '+str(publisher))



        # === Create a Consumer and queues - this will become part of define_subscription.

        #Create two test queues - don't use topics to test the consumer
        # To be replaced when the subscription service is ready
        queue1=dataobject.create_unique_identity()
        queue_properties = {queue1:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)

        queue2=dataobject.create_unique_identity()
        queue_properties = {queue2:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)

        pd1={'name':'example_consumer_1',
                 'module':'ion.services.dm.distribution.consumers.forwarding_consumer',
                 'procclass':'ForwardingConsumer',
                 'spawnargs':{'attach':topic.queue.name,\
                              'process parameters':{},\
                              'delivery queues':{'queues':[queue1,queue2]}}\
                    }
        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)

        # === End to be replaces with Define_Consumer


        # Create and send a data message
        data = {'Data':'in a dictionary'}
        result = yield dpsc.publish(self.sup, topic.reference(), data)
        if result:
            log.info('Published Message')
        else:
            log.info('Failed to Published Message')

        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)

        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(queue1),1)
        self.assertEqual(sent.get(queue2),1)
        self.assertEqual(received.get(topic.queue.name),1)


        # === Create a Consumer - this will become part of define_subscription.

        pd2={'name':'example_consumer_2',
                 'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'procclass':'LoggingConsumer',
                 'spawnargs':{'attach':queue1,\
                              'process parameters':{},\
                              'delivery queues':{}}\
                    }
        child2 = base_consumer.ConsumerDesc(**pd2)

        child2_id = yield self.test_sup.spawn_child(child2)

        # === End of what will become part of the subscription definition

        # Send the simple message again
        result = yield dpsc.publish(self.sup, topic.reference(), data)

        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)

        msg_cnt = yield child1.get_msg_count()

        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(queue1),2)
        self.assertEqual(sent.get(queue2),2)
        self.assertEqual(received.get(topic.queue.name),2)

        msg_cnt = yield child2.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(queue1),1)




    @defer.inlineCallbacks
    def test_exampleconsumer(self):
        '''
        @brief Example Consumer is a demonstration of a more complex data consumer.
        It uses DAP data messages and provides qaqc and event results on two
        seperate queues.
        '''
        dpsc = DataPubsubClient(self.sup)

        #Create and register 3 topics!
        topic_raw = PubSubTopicResource.create("topic_raw","oceans, oil spill, fun things to do")
        topic_raw = yield dpsc.define_topic(topic_raw)


        #Create and register self.sup as a publisher
        publisher = PublisherResource.create('Test Publisher', self.sup, topic_raw, 'DataObject')
        publisher = yield dpsc.define_publisher(publisher)

        log.info('Defined Publisher: '+str(publisher))

        # === Create a Consumer and queues - this will become part of define_subscription.

        #Create two test queues - don't use topics to test the consumer
        # To be replaced when the subscription service is ready
        evt_queue=dataobject.create_unique_identity()
        queue_properties = {evt_queue:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)

        pr_queue=dataobject.create_unique_identity()
        queue_properties = {pr_queue:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)

        pd1={'name':'example_consumer_1',
                 'module':'ion.services.dm.distribution.consumers.example_consumer',
                 'procclass':'ExampleConsumer',
                 'spawnargs':{'attach':topic_raw.queue.name,\
                              'Process Parameters':{},\
                              'delivery queues':\
                              {'event_queue':evt_queue,\
                               'processed_queue':pr_queue}}\
                    }

        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)


        pd2={'name':'example_consumer_2',
                 'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'procclass':'LoggingConsumer',
                 'spawnargs':{'attach':evt_queue,\
                              'Process Parameters':{}}\
                    }
        child2 = base_consumer.ConsumerDesc(**pd2)

        child2_id = yield self.test_sup.spawn_child(child2)

        pd3={'name':'example_consumer_3',
                 'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'procclass':'LoggingConsumer',
                 'spawnargs':{'attach':pr_queue,\
                              'Process Parameters':{}}\
                    }
        child3 = base_consumer.ConsumerDesc(**pd3)

        child3_id = yield self.test_sup.spawn_child(child3)

        # === End of stuff that will be replaced with Subscription method...


        # Create an example data message
        dmsg = dap_tools.simple_datamessage(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(101,102,103,104,105,106,107,108,109,110), \
            'height':(5,2,4,5,-1,9,3,888,3,4)})

        result = yield dpsc.publish(self.sup, topic_raw.reference(), dmsg)
        if result:
            log.info('Published Message')
        else:
            log.info('Failed to Published Message')


        # Need to await the delivery of data messages into the consumers
        yield pu.asleep(1)

        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(evt_queue),2)
        self.assertEqual(sent.get(pr_queue),1)
        self.assertEqual(received.get(topic_raw.queue.name),1)

        msg_cnt = yield child2.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(evt_queue),2)

        msg_cnt = yield child3.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(pr_queue),1)


        # Publish a second message with different data
        dmsg = dap_tools.simple_datamessage(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,986,4,-2,-1,5,3,1,4,5)})

        result = yield dpsc.publish(self.sup, topic_raw.reference(), dmsg)

        # Need to await the delivery of data messages into the consumers
        yield pu.asleep(1)

        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(evt_queue),5)
        self.assertEqual(sent.get(pr_queue),2)
        self.assertEqual(received.get(topic_raw.queue.name),2)

        msg_cnt = yield child2.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(evt_queue),5)

        msg_cnt = yield child3.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(pr_queue),2)
