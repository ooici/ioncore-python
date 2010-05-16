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
from magnet.store import Store

from ion.core import ioninit, base_process
from ion.core.cc.modloader import ModuleLoader
from ion.core.supervisor import Supervisor, ChildProcess
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
        if contargs.startswith('{}'):
            try:
                # Evaluate args and expect they are dict as str
                evargs = eval(contargs)
                logging.info('Evaluating args: '+str(evargs))
                if type(evargs) is dict:
                    ioninit.update(evargs)
            except Exception, e:
                logging.error('Invalid argument format: ', e)
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
        if scope == 'group':
            group = cgroup if cgroup else CONF['container_group']
            msgName =  group + "." + msgName

        # declare queues, bindings as needed
        logging.info("Messaging name config: name="+msgName+', '+str(msgResource))
        yield Container.configure_messaging(msgName, msgResource)
        
        # save name in the name registry
        yield nameRegistry.put(msgName, msgResource)

@defer.inlineCallbacks
def spawn_processes(procs):
    """
    Spawns a set of processes.
    @param procs  list of processes (as description dict) to start up
    @retval supervisor BaseProcess instance
    """
    children = prepare_childprocs(procs)

    # Makes the boostrap a process
    logging.info("Spawning bootstrap supervisor")
    sup = Supervisor()
    sup.receiver.label = Supervisor.__name__
    supId = yield sup.spawn()
    yield base_process.procRegistry.put("bootstrap", str(supId))

    sup.setChildProcesses(children)
    yield sup.spawnChildProcesses()
    for child in sup.childProcesses:
        procId = child.procId
        yield base_process.procRegistry.put(str(child.procName), str(procId))

    logging.debug("process_ids: "+ str(base_process.procRegistry.kvs))

    yield sup.initChildProcesses()
    defer.returnValue(sup)

def prepare_childprocs(procs):
    """
    Prepares a Supervisor class instance with configured child processes.
    """
    logging.info("Preparing bootstrap supervisor")
   
    children = []
    for procDef in procs:
        spawnArgs = procDef.get('spawnargs',None)

        child = ChildProcess(procDef['module'], procDef['class'], None, spawnArgs)
        child.procName = procDef['name']
        children.append(child)

    logging.debug("Supervisor child procs: "+str(children))
    return children

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
