#!/usr/bin/env python

"""
@file res/scripts/pubsub.py
@author David Stuebe
@brief main module for bootstrapping dm pubsub services
"""

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap

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
    
    print startsvcs
    print ion_messaging

    yield bootstrap.bootstrap(ion_messaging, startsvcs)

start()

