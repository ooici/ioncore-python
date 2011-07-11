#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@brief Main module for bootstrapping the system and support functions. Functions
        in here are called from ioncore application module and from test cases.
"""

from twisted.internet import defer, reactor
import os
import signal

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.cc.container import Container
from ion.core.cc.modloader import ModuleLoader
from ion.core.messaging.receiver import Receiver
from ion.core.process import process
from ion.core.process.process import Process, ProcessDesc
from ion.util.config import Config
import ion.util.procutils as pu

CONF = ioninit.config(__name__)

@defer.inlineCallbacks
def bootstrap(messaging=None, services=None):
    """
    @brief Initializes local container and starts services and messaging from
        given setup args.
    @param messaging  dict of messaging name configuration dicts (obsolete)
    @param services list of services (as svc description dict) to start up
    @retval Deferred -> supervisor Process instance
    """
    log.info("Init container, configuring messaging and starting services...")
    yield init_ioncore()
    sup = None
    if messaging:
        raise NotImplementedError("bootstrap: messaging configuration not supported")
        #assert type(messaging) is dict
        #yield declare_messaging(messaging)
    if services:
        assert type(services) is list
        sup = yield spawn_processes(services)

    defer.returnValue(sup)

def init_ioncore():
    """
    Performs global initializations on the local container on startup.
    @retval Deferred
    """
    # Extract command line args and set with Container instance
    _set_container_args(Container.args)

    # Collect all service declarations in local code modules
    #ModuleLoader().load_modules()


    # @todo Service registry call for local service/version registration
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
    Receiver.rec_messages.clear()
    Receiver.rec_shutoff = False
    from ion.core.cc.cc_agent import CCAgent
    CCAgent.instance = None

    # reset things set by _set_container_args
    #ioninit.cont_args.pop('contid', None)
    ioninit.cont_args.pop('sysname', None)

# Use SIGALARM to detect busy loops, where the reactor is not getting to process events.
# Monkey-patch the reactor to count iterations, to show it's not blocked. Int overflow shouldn't break it.
# This needs to only be done once per reactor, and not once per container, so it's package-level code.

if not 'ION_NO_BUSYLOOP_DETECT' in os.environ:
    reactorStepCount = 0
    _doIteration = getattr(reactor, 'doIteration')
    def doIterationWithCount(*args, **kwargs):
        global reactorStepCount
        reactorStepCount += 1
        _doIteration(*args, **kwargs)
    setattr(reactor, 'doIteration', doIterationWithCount)

    lastReactorStepCount = 0
    def alarm_handler(signum, frame):
        global lastReactorStepCount
        signal.alarm(1)

        if reactor.running and reactorStepCount == lastReactorStepCount:
            log.critical('Busy loop detected! The reactor has not run for >= 1 sec. ' +\
                         'Currently at %s():%d of file %s.' % (
                         frame.f_code.co_name, frame.f_lineno, frame.f_code.co_filename))

        lastReactorStepCount = reactorStepCount

    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(1)
