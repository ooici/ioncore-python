#!/usr/bin/env python

"""
@file res/scripts/eoi_agent_test.py
@author Chris Mueller
@brief main module for bootstrapping eoi_agent
"""

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('startup.bootstrap1')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('messaging_cfg', CONF)


@defer.inlineCallbacks
def start():
    """
    Main function of bootstrap. Starts EOI_AGENT system with static config
    """
    logging.info("ION/EOI_AGENT bootstrapping now...")
    startsvcs = []
 
 
    services = [
            {'name':'java_wrapper_agent','module':'ion.agents.eoiagents.java_wrapper_agent','class':'JavaWrapperAgent'}
            ]
 
    startsvcs.extend(services)

    yield bootstrap.bootstrap(agent_procs, startsvcs)

start()
