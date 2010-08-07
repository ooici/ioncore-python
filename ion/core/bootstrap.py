#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@brief main module for bootstrapping the system and support functions. Functions
        in here are actually called from start scripts and test cases.
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from magnet import spawnable
from magnet.container import Container
from magnet.spawnable import spawn
from ion.data.store import Store

from ion.core import ioninit, base_process
from ion.core.base_process import BaseProcess, ProcessDesc
from ion.core.cc.modloader import ModuleLoader

from ion.services.coi import service_registry
from ion.data.datastore import registry

from ion.util.config import Config
import ion.util.procutils as pu

CONF = ioninit.config(__name__)

# Static definition of message queues
ion_messaging = {}

# Static definition of service names
cc_agent = Config(CONF.getValue('ccagent_cfg')).getObject()
ion_core_services = Config(CONF.getValue('coreservices_cfg')).getObject()

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
            logging.info("Parsing KV")
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

# Sequence number of supervisors
sup_seq = 0

@defer.inlineCallbacks
def spawn_processes(procs, sup=None):
    """
    Spawns a set of processes.
    @param procs  list of processes (as description dict) to start up
    @param sup  spawned BaseProcess instance acting as supervisor
    @retval Deferred, for supervisor BaseProcess instance
    """
    children = []
    for procDef in procs:
        child = ProcessDesc(**procDef)
        children.append(child)

    if sup == None:
        sup = yield create_supervisor()

    logging.info("Spawning child processes")
    for child in children:
        child_id = yield sup.spawn_child(child)

    logging.debug("process_ids: "+ str(base_process.procRegistry.kvs))

    defer.returnValue(sup)

@defer.inlineCallbacks
def create_supervisor():
    """
    Creates a supervisor process
    @retval Deferred, for supervisor BaseProcess instance
    """
    global sup_seq
    # Makes the boostrap a process
    logging.info("Spawning supervisor")
    if sup_seq == 0:
        supname = "bootstrap"
    else:
        supname = "supervisor."+str(sup_seq)
    suprec = base_process.factory.build({'proc-name':supname})
    sup = suprec.procinst
    sup.receiver.group = supname
    supId = yield sup.spawn()
    yield base_process.procRegistry.put(supname, str(supId))
    sup_seq += 1
    defer.returnValue(sup)


@defer.inlineCallbacks
def bs_register_services():
    """
    Register all the declared processes.
    """
    src = service_registry.ServiceRegistryClient()
    for proc in base_process.processes.values():
        sd = service_registry.ServiceDesc()
        sd.name = proc['name']
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
    base_process.processes = {}
    base_process.receivers = []
    spawnable.store = Container.store
    spawnable.Spawnable.progeny = {}


"""
from ion.core import bootstrap as b
b.start()
"""
