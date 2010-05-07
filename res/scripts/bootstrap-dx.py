#!/usr/bin/env python

"""
@file ion/core/bootstrap-dx.py
@author Michael Meisinger
@brief main module for bootstrapping data exchange
"""

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config

CONF = ioninit.config('ion.core.bootstrap-dx')
# Static definition of message queues
ion_messaging = Config(CONF.getValue('messaging_cfg')).getObject()

# Static definition of service names
dx_services = Config(CONF.getValue('services_cfg')).getObject()

@defer.inlineCallbacks
def start():
    """
    Main function of bootstrap. Starts DX system with static config
    """
    logging.info("ION/DX bootstrapping now...")
    startsvcs = []
    startsvcs.extend(ion_services)

    yield bootstrap.bootstrap(ion_messaging, startsvcs)
