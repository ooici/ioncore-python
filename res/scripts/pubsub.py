#!/usr/bin/env python

"""
@file res/scripts/pubsub.py
@author David Stuebe
@brief main module for bootstrapping dm pubsub services
"""
import random

import logging
from twisted.internet import defer

from ion.core.base_process import BaseProcess, ProcessDesc
from ion.core import ioninit
from ion.core import bootstrap

from ion.services.dm.distribution import pubsub_service

from ion.resources.dm_resource_descriptions import PubSubTopicResource, SubscriptionResource

from ion.services.dm.distribution.consumers import example_consumer, forwarding_consumer, latest_consumer, logging_consumer

CONF = ioninit.config('startup.pubsub')

# Static definition of message queues
ion_messaging = ioninit.get_config('messaging_cfg', CONF)

# Static definition of service names
dm_services = ioninit.get_config('services_cfg', CONF)


# Static definition of service names
#dm_services = Config(CONF.getValue('services_cfg')).getObject()
#ion_messaging = Config(CONF.getValue('messaging_cfg')).getObject()

@defer.inlineCallbacks
def create_producers(proc,n=1):
    
    dpsc = pubsub_service.DataPubsubClient(proc=proc)
    
    for i in range(n):
         
        tname = 'topic name '+str(i)
        ka = 'keyword a'
        kb = 'keyword b'
    
        if (i/2)*2 == i:
            keyword = ka
        else:
            keyword = kb
        topic = PubSubTopicResource.create(tname,keyword)
        topic = yield dpsc.define_topic(topic)

            
        dspname = 'data_stream_producer_'+str(i)
        interval = random.randint(1,10)
        dsp={'name':dspname,
                    'module':'ion.services.dm.util.data_stream_producer',
                    'procclass':'DataStreamProducer',
                    'spawnargs':{'delivery queue':topic.queue.name,
                                 'delivery interval':interval}}
        
        child = ProcessDesc(**dsp)
        child_id = yield proc.spawn_child(child)

@defer.inlineCallbacks
def start():
    """
    Main function of bootstrap. Starts DM pubsub...
    """
    logging.info("ION DM PubSub bootstrapping now...")
    startsvcs = []
    startsvcs.extend(dm_services)
    sup = yield bootstrap.bootstrap(ion_messaging, startsvcs)
        
    print 'STARTSVCS',startsvcs
    print 'ION_MESSAGING',ion_messaging
    print 'CONT_ARGS',ioninit.cont_args
    
    nproducers = ioninit.cont_args.get('nproducers',5)
    print 'NPRODUCERS',nproducers
    yield create_producers(sup, nproducers)
        
    dpsc = pubsub_service.DataPubsubClient(proc=sup)
    
        
    subscription = SubscriptionResource()
    subscription.topic1 = PubSubTopicResource.create('topic','')
    

    # Use the example consumer to create events...
    subscription.workflow = {
        'consumer1':
            {'module':'ion.services.dm.distribution.consumers.example_consumer',
                'consumerclass':'ExampleConsumer',\
                'attach':'topic1'},
        'consumer2':
            {'module':'ion.services.dm.distribution.consumers.logging_consumer',
                'consumerclass':'LoggingConsumer',\
                'attach':[['consumer1','event_queue']]}
            }
    
    """
    # Just use the logging consumer
    subscription.workflow = {
        'consumer1':
            {'module':'ion.services.dm.distribution.consumers.logging_consumer',
                'consumerclass':'LoggingConsumer',\
                'attach':'topic1'}
            }
    """
    
    
    subscription = yield dpsc.define_subscription(subscription)
    

start()

