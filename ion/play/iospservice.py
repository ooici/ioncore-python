# Python Capability Container start script.
# Starts container with Java Services.

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config
from ion.services.coi.attributestore import AttributeStoreClient

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('ccagent_cfg', CONF)
demo_procs = [
    {'name':'attributestore','module':'ion.services.coi.attributestore','class':'AttributeStoreService', 'spawnargs':{'argname':'argument'}},

    {'name':'javaint','module':'ion.demo.lca.javaint_service','class':'JavaIntegrationService'},
]

@defer.inlineCallbacks
def main(ns={}):
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing... [LCA Java Integration Demo]")

    processes = []
    processes.extend(demo_procs)
    
    # Start the processes
    sup = yield bootstrap.bootstrap(None, processes)
    asc = AttributeStoreClient(proc=sup)
    
    ns.update(locals())
    
# main(locals())
