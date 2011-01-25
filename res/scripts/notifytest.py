#!/usr/bin/env python

"""
@file res/scripts/notifytest.py
@author Dave Foster <dfoster@asascience.com>
@brief pubsub notification tests
"""
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.core.cc.shell import control
from ion.util import ionlog

from ion.services.dm.distribution.notification import LoggingReceiver, LoggingHandler

CONF = ioninit.config('startup.pubsub')

# Static definition of message queues
ion_messaging = ioninit.get_config('messaging_cfg', CONF)

notify_services = [
 ]

@defer.inlineCallbacks
def start():
    """
    Main function of bootstrap.
    """
    startsvcs = []
    startsvcs.extend(notify_services)
    sup = yield bootstrap.bootstrap(ion_messaging, startsvcs)

    ionlog.log_factory.add_handler(LoggingHandler(ioninit.container_instance.exchange_manager.exchange_space, {}))

    lr = LoggingReceiver("nobody", loglevel="DEBUG")
    lr.attach()

    #lp = yield LoggingPublisher.name(ioninit.container_instance.exchange_manager.exchange_space, {}, name="testlogger")

    control.add_term_name("lr", lr)
    #control.add_term_name("lp", lp)
    print "'lr' available."

start()

