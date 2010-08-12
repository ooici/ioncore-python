# Python Capability Container start script.
# Starts an instrument agent/driver combination for one instrance of a sensor (simulator).

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('ccagent_cfg', CONF)

service_procs = [
    {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
    {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'}
    ]

start_procs = [
    {'name':'SBE49_Driver','module':'ion.agents.instrumentagents.SBE49_driver','class':'SBE49InstrumentDriver'},
]

@defer.inlineCallbacks
def main():
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing... [Start an Instrument Agent]")

    processes = []
    processes.extend(agent_procs)
    processes.extend(service_procs)
    processes.extend(start_procs)

    # Start the processes
    sup = yield bootstrap.bootstrap(None, processes)

main()
