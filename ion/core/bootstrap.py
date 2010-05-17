#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@brief main module for bootstrapping the system and support functions. Functions
        in here are actually called from start scripts and test cases.
"""

import logging
from twisted.internet import defer

from magnet import spawnable
from magnet.container import Container
from magnet.spawnable import spawn
from ion.data.store import Store

from ion.core import ioninit, base_process
from ion.core.base_process import BaseProcess, ProcessDesc
from ion.core.cc.modloader import ModuleLoader
from ion.services.coi.service_registry import ServiceRegistryClient, ServiceDesc
from ion.util.config import Config
import ion.util.procutils as pu

CONF = ioninit.config(__name__)

# Static definition of message queues
ion_messaging = {}

# Static definition of service names
ion_core_services = Config(CONF.getValue('coreservices_cfg')).getObject()
ion_services = Config(CONF.getValue('services_cfg')).getObject()

# Messaging names
nameRegistry = Store()

@defer.inlineCallbacks
def start():
    """
    Starts ION system with static config of core messaging names and services.
    """
    logging.info("ION SYSTEM bootstrapping now...")
    startsvcs = []
    startsvcs.extend(ion_core_services)
    sup = yield bootstrap(ion_messaging, startsvcs)

@defer.inlineCallbacks
def bootstrap(messaging=None, services=None):
    """
    Initializes local container and starts services and messaging from given
    setup args.
    @param messaging  dict of messaging name configuration dicts
    @param services list of services (as svc description dict) to start up
    @retval supervisor BaseProcess instance
    """
    logging.info("Init container, configuring messaging and starting services...")
    init_container()
    sup = None
    if messaging:
        assert type(messaging) is dict
        yield declare_messaging(messaging)
    if services:
        assert type(services) is list
        sup = yield spawn_processes(services)

    defer.returnValue(sup)

def init_container():
    """
    Performs global initializations on the local container on startup.
    """
    _set_container_args(Container.args)
    interceptorsys = CONF.getValue('interceptor_system',None)
    if interceptorsys:
        logging.info("Setting capability container interceptor system")
        cls = pu.get_class(interceptorsys)
        Container.interceptor_system = cls()
    # Collect all service declarations in local code modules
    ModuleLoader().load_modules()
    #yield bs_register_services()

def _set_container_args(contargs=None):
    ioninit.cont_args['_args'] = contargs
    if contargs:
        logging.info('Evaluating and setting container args: '+str(contargs))
        if contargs.startswith('{'):
            try:
                # Evaluate args and expect they are dict as str
                evargs = eval(contargs)
                if type(evargs) is dict:
                    ioninit.cont_args.update(evargs)
            except Exception, e:
                logging.error('Invalid argument format: ', e)
        elif contargs.find('=') > 0:
            # Key=value arguments separated by comma
            print "Parsing KV"
            args = contargs.split(',')
            for a in args:
                k,s,v = a.partition('=')
                ioninit.cont_args[k.strip()] = v.strip()
        else:
            ioninit.cont_args['args'] = contargs
            
@defer.inlineCallbacks
def declare_messaging(messagingCfg, cgroup=None):
    """
    Configures messaging resources.
    @todo this needs to go to the exchange registry service
    """
    # for each messaging resource call Magnet to define a resource
    for name, msgResource in messagingCfg.iteritems():
        scope = msgResource.get('args',{}).get('scope','global')
        msgName = name
        if scope == 'local':
            msgName = Container.id + "." + msgName
        elif scope == 'system':
            # @todo: in the root bootstrap this is ok, but HACK
            msgName = Container.id + "." + msgName

        # declare queues, bindings as needed
        logging.info("Messaging name config: name="+msgName+', '+str(msgResource))
        yield Container.configure_messaging(msgName, msgResource)
        
        # save name in the name registry
        yield nameRegistry.put(msgName, msgResource)

# Sequence number of supervisors
sup_seq = 0

@defer.inlineCallbacks
def spawn_processes(procs, sup=None):
    """
    Spawns a set of processes.
    @param procs  list of processes (as description dict) to start up
    @param sup  spawned BaseProcess instance acting as supervisor
    @retval supervisor BaseProcess instance
    """
    global sup_seq
    children = []
    for procDef in procs:
        spawnArgs = procDef.get('spawnargs', None)
        child = ProcessDesc(procDef['name'], procDef['module'], procDef['class'], None, spawnArgs)
        children.append(child)

    if not sup:
        # Makes the boostrap a process
        logging.info("Spawning supervisor")
        if sup_seq == 0:
            supname = "bootstrap"
        else:
            supname = "supervisor."+str(sup_seq)
        sup = BaseProcess()
        sup.receiver.label = supname
        sup.receiver.group = supname
        supId = yield sup.spawn()
        yield base_process.procRegistry.put(supname, str(supId))
        sup_seq += 1

    logging.info("Spawning child processes")
    for child in children:
        child_id = yield sup.spawn_child(child)
        yield base_process.procRegistry.put(str(child.procName), str(child_id))

    logging.debug("process_ids: "+ str(base_process.procRegistry.kvs))

    defer.returnValue(sup)

@defer.inlineCallbacks
def bs_register_services():
    """
    Register all the declared processes.
    """
    src = ServiceRegistryClient()
    for proc in base_process.processes.values():
        sd = ServiceDesc(name=proc['name'])
        res = yield src.register_service(sd)


def reset_container():
    """
    Resets the container for warm restart. Simple implementation
    currently. Used for testing only.
    """
    # The following is extremely hacky. Reset static module and classvariables
    # to their defaults. Even further, reset imported names in other modules
    # to the new objects.
    base_process.procRegistry = Store()
    nameRegistry = Store()
    spawnable.store = Container.store
    spawnable.Spawnable.progeny = {}


"""
from ion.core import bootstrap as b
b.start()
"""
