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

from ion.core.supervisor import Supervisor, ChildProcess
from ion.util.config import Config
import ion.util.procutils as pu

logging.basicConfig(level=logging.DEBUG)

# Static definition of message queues
ion_queues = {}

# Static definition of service names
ion_services = Config('res/config/ionservices.cfg').getObject()

# Local process ids
process_ids = Store()

@defer.inlineCallbacks
def start():
    """Main function of bootstrap. Starts system with static config
    """
    yield _bootstrap(ion_queues,ion_services)

@defer.inlineCallbacks
def _bootstrap(queues, procs):
    """Bootstraps the system from a configuration 
    """
    logging.info("ION SYSTEM bootstrapping now...")
    yield bs_processes(procs)

@defer.inlineCallbacks
def bs_processes(procs):
    """Bootstraps a set of processes 
    """
    sup = bs_prepSupervisor(procs)

    logging.info("Spawning bootstrap supervisor")
    supId = yield spawn(sup.receiver)
    yield process_ids.put("bootstrap", supId)

    yield sup.spawnChildProcesses()
    for child in sup.childProcesses:
        procId = child.procId
        yield process_ids.put(child.procName, procId)

    logging.debug("process_ids: "+ str(process_ids.kvs))

    yield sup.initChildProcesses()

def bs_prepSupervisor(procs):
    """Prepares a Supervisor class instance with configured child processes.
    """
    logging.info("Preparing bootstrap supervisor")
   
    children = []
    for procName, procDef in procs.iteritems():
        child = ChildProcess(procDef['module'], procDef['class'], None)
        child.procName = procName
        children.append(child)

    logging.debug("Supervisor child procs: "+str(children))

    sup = Supervisor()
    sup.childProcesses = children
    return sup

"""
from ion.core import bootstrap as b
b.start()
"""
