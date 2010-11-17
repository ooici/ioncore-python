#!/usr/bin/env python

"""
@file res/scripts/pubsub.py
@author David Stuebe
@brief main module for bootstrapping dm pubsub services test
This test creates a configurable number of publisher processes using the
nproducers argument from the command line. The default configuration of the
script creates a subscription workflow which process the results and creates
an aggregate statement in the log about the number of data events.
"""
import random

import logging
from twisted.internet import defer

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
def create_producer(proc):

    dpsc = pubsub_service.DataPubsubClient(proc=proc)


    topic = PubSubTopicResource.create('time series','dap grid, timeseries, data')
    topic = yield dpsc.define_topic(topic)


    dspname = 'data_timeseries_stream_producer'
    dsp={'name':dspname,
            'module':'ion.services.dm.util.dap_grid_timeseries_producer',
            'procclass':'DapGridDataStreamProducer',
            'spawnargs':{'delivery queue':topic.queue.name,
                         'delivery interval':5}
        }

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

    yield create_producer(sup)

    dpsc = pubsub_service.DataPubsubClient(proc=sup)

    subscription = SubscriptionResource()
    subscription.topic1 = PubSubTopicResource.create('time series','')

    # Use the example consumer to create events... graph the number of events
    subscription.workflow = {
        'consumer1':
            {'module':'ion.services.dm.distribution.consumers.timeseries_consumer',
                'consumerclass':'TimeseriesConsumer',\
                'attach':'topic1'},
        'consumer3':
            {'module':'ion.services.dm.presentation.web_viz_consumer',
                'consumerclass':'WebVizConsumer',\
                'attach':[['consumer1','queue']],
                'process parameters':{'port':8180}
            }

        }


    subscription = yield dpsc.define_subscription(subscription)
    linfo = '\n================================================\n'
    linfo+= 'Open your web browser and look at: http://127.0.0.1:8180/ \n'
    linfo+= '================================================\n'
    logging.info(linfo)



start()
