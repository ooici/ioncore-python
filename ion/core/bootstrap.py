#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@brief main module for bootstrapping the system and support functions. Functions
        in here are actually called from ioncore application module and test cases.
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.cc.container import Container
from ion.core.cc.modloader import ModuleLoader
from ion.core.process import process
from ion.core.process.process import Process, ProcessDesc
from ion.data.store import Store
from ion.data.datastore import registry
from ion.resources import description_utility
from ion.services.coi import service_registry

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
    log.info("ION SYSTEM bootstrapping now...")
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
    @retval Deferred -> supervisor Process instance
    """
    log.info("Init container, configuring messaging and starting services...")
    yield init_ioncore()
    sup = None
    if messaging:
        assert type(messaging) is dict
        yield declare_messaging(messaging)
    if services:
        assert type(services) is list
        sup = yield spawn_processes(services)

    defer.returnValue(sup)

def init_ioncore():
    """
    Performs global initializations on the local container on startup.
    @retval Deferred
    """
    _set_container_args(Container.args)
    #interceptorsys = CONF.getValue('interceptor_system',None)
    #if interceptorsys:
    #    log.info("Setting capability container interceptor system")
    #    cls = pu.get_class(interceptorsys)
    #    Container.interceptor_system = cls()
    # Collect all service declarations in local code modules
    ModuleLoader().load_modules()

    #Load All Resource Descriptions for future decoding
    description_utility.load_descriptions()

    #yield bs_register_services()
    return defer.succeed(None)

def _set_container_args(contargs=None):
    ioninit.cont_args['_args'] = contargs
    if contargs:
        log.info('Evaluating and setting container args: '+str(contargs))
        if contargs.startswith('{'):
            try:
                # Evaluate args and expect they are dict as str
                evargs = eval(contargs)
                if type(evargs) is dict:
                    ioninit.cont_args.update(evargs)
            except Exception, e:
                log.error('Invalid argument format: ', e)
        elif contargs.find('=') > 0:
            # Key=value arguments separated by comma
            log.info("Parsing KV")
            args = contargs.split(',')
            for a in args:
                k,s,v = a.partition('=')
                ioninit.cont_args[k.strip()] = v.strip()
        else:
            ioninit.cont_args['args'] = contargs
    if 'contid' in ioninit.cont_args:
        Container.id = ioninit.cont_args['contid']
    if 'sysname' in ioninit.cont_args:
        ioninit.sys_name = ioninit.cont_args['sysname']
    else:
        ioninit.sys_name = ioninit.container_instance.id

def declare_messaging(messagingCfg, cgroup=None):
    return ioninit.container_instance.declare_messaging(messagingCfg, cgroup)

def spawn_processes(procs, sup=None):
    return ioninit.container_instance.spawn_processes(procs, sup)

def create_supervisor():
    return ioninit.container_instance.create_supervisor()

'''
This method is out of date with the service registry
@defer.inlineCallbacks
def bs_register_services():
    """
    Register all the declared processes.
    """
    src = service_registry.ServiceRegistryClient()
    for proc in process.processes.values():
        sd = service_registry.ServiceDesc()
        sd.name = proc['name']
        res = yield src.register_service(sd)
'''
def reset_container():
    """
    Resets the container for warm restart. Simple implementation
    currently. Used for testing only.
    """
    # The following is extremely hacky. Reset static module and classvariables
    # to their defaults. Even further, reset imported names in other modules
    # to the new objects.
    process.procRegistry.kvs.clear()
    process.processes.clear()
