#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_pubsub.py
@author Michael Meisinger
@author David Stuebe
@brief test service for registering topics for data publication & subscription
"""

import logging
logging = logging.getLogger(__name__)
#import time
from twisted.internet import defer
from twisted.trial import unittest
#from magnet.container import Container
#from magnet.spawnable import Receiver
#from magnet.spawnable import spawn

#from ion.core.base_process import ProtocolFactory
from ion.core import bootstrap
#from ion.core.base_process import BaseProcess
from ion.services.dm.distribution.pubsub_service import DataPubsubClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource, DAPMessageObject

from ion.services.dm.util import dap_tools

from ion.services.dm.distribution import base_consumer
from ion.services.dm.distribution.consumers import forwarding_consumer
from ion.services.dm.distribution.consumers import logging_consumer
from ion.services.dm.distribution.consumers import example_consumer

#import numpy

from ion.services.dm.util import dap_tools


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
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_pubsub(self):

        dpsc = DataPubsubClient(self.sup)
        
        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")        
        topic = yield dpsc.define_topic(topic)
        logging.info('Defined Topic: '+str(topic))

        #Create and register self.sup as a publisher
        print 'SUP',self.sup,self.test_sup
        
        publisher = PublisherResource.create('Test Publisher', self.sup, topic, 'DataObject')
        publisher = yield dpsc.define_publisher(publisher)

        logging.info('Defined Publisher: '+str(publisher))
        

        
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
                              'Process Parameters':{},\
                              'delivery queues':{'queues':[queue1,queue2]}}\
                    }
        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)

        # === End to be replaces with Define_Consumer


        # Create and send a data message
        data = {'Data':'in a dictionary'}
        result = yield dpsc.publish(self.sup, topic.reference(), data)
        if result:
            logging.info('Published Message')
        else:
            logging.info('Failed to Published Message')

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
                 'spawnargs':{'attach':queue1}\
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
        @Brief Example Consumer is a demonstration of a more complex data consumer.
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

        logging.info('Defined Publisher: '+str(publisher))

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
            logging.info('Published Message')
        else:
            logging.info('Failed to Published Message')


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


