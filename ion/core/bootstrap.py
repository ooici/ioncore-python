#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@brief main module for bootstrapping the system
"""

import logging
from twisted.internet import defer
import time

from magnet.container import Container
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
ion_messaging = {}

# Static definition of service names
ion_core_services = Config(CONF.getValue('coreservices_cfg')).getObject()
ion_services = Config(CONF.getValue('services_cfg')).getObject()

# Local process ids
process_ids = procRegistry

# Messaging names
nameRegistry = Store()

@defer.inlineCallbacks
def start():
    """Main function of bootstrap. Starts system with static config of
    messaging names and services
    """
    startsvcs = []
    startsvcs.extend(ion_core_services)
    #startsvcs.extend(ion_services)
    yield bootstrap(ion_messaging, startsvcs)

@defer.inlineCallbacks
def bootstrap(messaging, services):
    """Starts services and messaging from given setup
    """
    logging.info("ION SYSTEM bootstrapping now...")
    if messaging and len(messaging)>0:
        yield bs_messaging(messaging)
    if services and len(services)>0:
        yield bs_processes(services)

@defer.inlineCallbacks
def bs_messaging(messagingCfg, cgroup=None):
    """Configures the container messaging resources 
    """
    # for each messaging resource call Magnet to define a resource
    for name, msgResource in messagingCfg.iteritems():
        scope = msgResource.get('args',{}).get('scope','global')
        msgName = name
        if scope == 'local':
            msgName = Container.id + "." + msgName
        if scope == 'group':
            group = cgroup if cgroup else CONF['container_group']
            msgName =  group + "." + msgName

        # declare queues, bindings as needed
        logging.info("Messaging name config: name="+msgName+', '+str(msgResource))
        yield Container.configure_messaging(msgName, msgResource)
        
        # save name is the name registry
        yield nameRegistry.put(msgName, msgResource)

@defer.inlineCallbacks
def bs_processes(procs):
    """Bootstraps a set of processes 
    """
    sup = prepare_supervisor(procs)

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

def prepare_supervisor(procs):
    """Prepares a Supervisor class instance with configured child processes.
    """
    logging.info("Preparing bootstrap supervisor")
   
    children = []
    for procDef in procs:
        spawnArgs = procDef.get('spawnargs',None)

        child = ChildProcess(procDef['module'], procDef['class'], None, spawnArgs)
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
