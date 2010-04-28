#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@brief main module for bootstrapping the system
"""

import logging
from twisted.internet import defer
import time

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.core import ioninit
from ion.core.supervisor import Supervisor, ChildProcess
from ion.core.base_process import procRegistry
from ion.util.config import Config
import ion.util.procutils as pu

CONF = ioninit.config(__name__)

# Static definition of message queues
ion_queues = {}

# Static definition of service names
ion_core_services = Config(CONF.getValue('coreservices_cfg')).getObject()
ion_services = Config(CONF.getValue('services_cfg')).getObject()

# Local process ids
process_ids = procRegistry

@defer.inlineCallbacks
def start():
    """Main function of bootstrap. Starts system with static config
    """
    startsvcs = []
    startsvcs.extend(ion_core_services)
    #startsvcs.extend(ion_services)
    yield bootstrap_core_services()

@defer.inlineCallbacks
def bootstrap_core_services():
    """Starts core system services and messaging setup
    """
    logging.info("ION SYSTEM bootstrapping now...")
    yield bs_processes(ion_core_services)


@defer.inlineCallbacks
def bs_processes(procs):
    """Bootstraps a set of processes 
    """
    sup = bs_prepSupervisor(procs)

    # Makes the boostrap a process
    logging.info("Spawning bootstrap supervisor")
    supId = yield spawn(sup.receiver)
    yield process_ids.put("bootstrap", str(supId))

    yield sup.spawnChildProcesses()
    for child in sup.childProcesses:
        procId = child.procId
        yield process_ids.put(str(child.procName), str(procId))

    logging.debug("process_ids: "+ str(process_ids.kvs))

    yield sup.initChildProcesses()

def bs_prepSupervisor(procs):
    """Prepares a Supervisor class instance with configured child processes.
    """
    logging.info("Preparing bootstrap supervisor")
   
    children = []
    for procDef in procs:
        child = ChildProcess(procDef['module'], procDef['class'], None)
        child.procName = procDef['name']
        children.append(child)

    logging.debug("Supervisor child procs: "+str(children))

    sup = Supervisor()
    sup.childProcesses = children
    return sup

"""
from ion.core import bootstrap as b
b.start()
"""
