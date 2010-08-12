# Python Capability Container start script.
# Starts an instrument agent/driver combination for one instrance of a sensor (simulator).

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config

from subprocess import Popen, PIPE
import ion.util.procutils as pu
import os

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

    """
    Construct the path to the instrument simulator, starting with the current
    working directory
    """
    cwd = os.getcwd()
    myPid = os.getpid()
    logging.debug("DHE: myPid: %s" % (myPid))
    
    simDir = cwd + "/ion/agents/instrumentagents/test/"
    simPath = simDir + "sim_SBE49.py"
    logPath = simDir + "sim.log"
    logging.info("cwd: %s, simPath: %s, logPath: %s" %(str(cwd), str(simPath), str(logPath)))
    simLogObj = open(logPath, 'a')
    simProc = Popen(simPath, stdout=simLogObj)

    # Sleep for a while to allow simlator to get set up.
    yield pu.asleep(2)

    processes = []
    processes.extend(agent_procs)
    processes.extend(service_procs)
    processes.extend(start_procs)

    # Start the processes
    sup = yield bootstrap.bootstrap(None, processes)

main()
