# Python Capability Container start script.
# Starts container with Java Services.

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('ccagent_cfg', CONF)
demo_procs = [
    {'name':'instrument_registry','module':'ion.services.sa.instrument_registry','class':''},
    {'name':'data_product_registry','module':'ion.services.sa.data_product_registry','class':''},
    {'name':'instrument_management','module':'ion.services.sa.instrument_management','class':''},
    {'name':'service_registry','module':'ion.services.coi.service_registry','class':''},
    {'name':'registry','module':'ion.data.datastore.registry','class':'RegistryService', 'spawnargs':{'servicename':'registry'}},
    {'name':'javaint','module':'ion.demo.lca.javaint_service','class':'JavaIntegrationService'},
]

@defer.inlineCallbacks
def main():
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing... [LCA Java Integration Demo]")

    processes = []
    processes.extend(agent_procs)
    processes.extend(demo_procs)

    # Start the processes
    sup = yield bootstrap.bootstrap(None, processes)

main()
