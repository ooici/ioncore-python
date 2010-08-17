# Python Capability Container start script.
# Starts an instrument agent/driver combination for one instrance of a sensor (simulator).

import logging
from twisted.internet import defer

from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator

from ion.core import ioninit
from ion.core import bootstrap
from ion.services.dm.distribution.pubsub_service import DataPubsubClient
from ion.util.config import Config
import ion.util.procutils as pu

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('ccagent_cfg', CONF)

service_procs = [
    {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
    {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'},
    {'name':'agent_registry','module':'ion.services.coi.agent_registry','class':'ResourceRegistryService'}
    ]

# Startup arguments
INSTRUMENT_ID = None

def eval_start_arguments():
    global INSTRUMENT_ID
    INSTRUMENT_ID = ioninit.cont_args.get('instid','123')
    print "##### Use instrument ID: " + str(INSTRUMENT_ID)

@defer.inlineCallbacks
def main():
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing... [Start an Instrument Agent]")

    processes = []
    processes.extend(agent_procs)
    processes.extend(service_procs)

    # Start the processes
    sup = yield bootstrap.bootstrap(None, processes)

    eval_start_arguments()

    simulator = Simulator(INSTRUMENT_ID, 9000)
    simulator.start()

    ia_procs = [
        {'name':'SBE49IA','module':'ion.agents.instrumentagents.SBE49_IA','class':'SBE49InstrumentAgent','spawnargs':{'instrument-id':INSTRUMENT_ID}},
    ]
    yield bootstrap.spawn_processes(ia_procs, sup=sup)

    ia_pid = sup.get_child_id('SBE49IA')
    iaclient = InstrumentAgentClient(proc=sup, target=ia_pid)
    yield iaclient.register_resource(INSTRUMENT_ID)

main()
