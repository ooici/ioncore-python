# Python Capability Container start script.
# Starts empty container with system name set.

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('ccagent_cfg', CONF)
svc_procs = ioninit.get_config('services_cfg', CONF)

@defer.inlineCallbacks
def main():
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing...")

    processes = []
    processes.extend(agent_procs)
    processes.extend(svc_procs)

    # Start the cc-agent and processes from config file
    sup = yield bootstrap.bootstrap(None, processes)

    # Start processes from config file given in arguments
    procsfile = ioninit.cont_args.get('processes', None)
    if procsfile:
        procs = Config(procsfile).getObject()
        logging.info("Starting local process list: "+str(procs))
        yield bootstrap.spawn_processes(procs, sup=sup)

main()
