# Python Capability Container start script.
# Starts an instrument agent/driver combination for one instrance of a sensor (simulator).

import logging
import os
from subprocess import Popen, PIPE
from twisted.internet import defer

from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
# FIX: Not good to import a test
from ion.agents.instrumentagents.test import test_SBE49

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

def start_simulator():
    """
    Construct the path to the instrument simulator, starting with the current
    working directory
    """
    cwd = os.getcwd()
    myPid = os.getpid()
    logging.debug("DHE: myPid: %s" % (myPid))

    simDir = cwd + "/ion/agents/instrumentagents/test/"
    simPath = simDir + "sim_SBE49.py"
    logPath = cwd + "/logs/sim_%s.log" % (INSTRUMENT_ID)
    logging.info("cwd: %s, simPath: %s, logPath: %s" %(str(cwd), str(simPath), str(logPath)))
    simLogObj = open(logPath, 'a')
    simProc = Popen([simPath,INSTRUMENT_ID], stdout=simLogObj)

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

    start_simulator()

    # Sleep for a while to allow simlator to get set up.
    yield pu.asleep(1)

    ia_procs = [
        {'name':'SBE49IA','module':'ion.agents.instrumentagents.SBE49_IA','class':'SBE49InstrumentAgent','spawnargs':{'instrument-id':INSTRUMENT_ID}},
    ]
    yield bootstrap.spawn_processes(ia_procs, sup=sup)

    ia_pid = sup.get_child_id('SBE49IA')
    iaclient = InstrumentAgentClient(proc=sup,target=ia_pid)
    yield iaclient.register_resource(INSTRUMENT_ID)

main()
