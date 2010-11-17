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



start()
