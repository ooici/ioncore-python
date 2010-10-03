#!/usr/bin/env python

"""
@file ion/core/process/process.py
@author Michael Meisinger
@brief base classes for processes within a capability container
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.id import Id
from ion.core.messaging.receiver import ProcessReceiver
import ion.util.procutils as pu

# @todo HACK: Dict of process "alias" to process declaration
processes = {}

class IProcess(Interface):
    """
    Interface for all capability container processes
    """

    def spawn():
        pass

class ProcessDesc(object):
    """
    Class that encapsulates attributes about a spawnable process; can spawn
    and init processes.
    """
    def __init__(self, **kwargs):
        """
        Initializes ProcessDesc instance with process attributes
        @param name  name label of process
        @param module  module name of process module
        @param class  or procclass is class name in process module (optional)
        @param node  ID of container to spawn process on (optional)
        @param spawnargs  dict of additional spawn arguments (optional)
        """
        self.proc_name = kwargs.get('name', None)
        self.proc_module = kwargs.get('module', None)
        self.proc_class = kwargs.get('class', kwargs.get('procclass', None))
        self.proc_node = kwargs.get('node', None)
        self.spawn_args = kwargs.get('spawnargs', None)
        self.proc_id = None
        self.proc_state = 'DEFINED'

    @defer.inlineCallbacks
    def spawn(self, supProc=None):
        """
        Spawns this process description with the initialized attributes.
        @param supProc  the process instance that should be set as supervisor
        """
        assert self.proc_state == 'DEFINED', "Cannot spawn process twice"
        self.sup_process = supProc
        if self.proc_node == None:
            log.info('Spawning name=%s node=%s' %
                         (self.proc_name, self.proc_node))

            # Importing service module
            proc_mod = pu.get_module(self.proc_module)
            self.proc_mod_obj = proc_mod

            # Spawn instance of a process
            # During spawn, the supervisor process id, system name and proc name
            # get provided as spawn args, in addition to any give spawn args.
            spawnargs = {'proc-name':self.proc_name,
                         'sup-id':self.sup_process.id.full,
                         'sys-name':self.sup_process.sys_name}
            if self.spawn_args:
                spawnargs.update(self.spawn_args)
            #log.debug("spawn(%s, args=%s)" % (self.proc_module, spawnargs))
            process = yield ioninit.container_instance.spawn_process(proc_mod, None, spawnargs)
            self.proc_id = process.id
            self.proc_state = 'SPAWNED'

            log.info("Process %s ID: %s" % (self.proc_class, self.proc_id))
        else:
            log.error('Cannot spawn '+self.proc_class+' on node='+str(self.proc_node))
            
        defer.returnValue(self.proc_id)

    @defer.inlineCallbacks
    def init(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'init', {}, {'quiet':True})
        if content.get('status','ERROR') == 'OK':
            self.proc_state = 'INIT_OK'
        else:
            self.proc_state = 'INIT_ERROR'

    @defer.inlineCallbacks
    def shutdown(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'shutdown', {}, {'quiet':True})
        if content.get('status','ERROR') == 'OK':
            self.proc_state = 'TERMINATED'
        else:
            self.proc_state = 'SHUTDOWN_ERROR'

class IProcessFactory(Interface):

    def build(spawnargs, container):
        """
        """

class ProcessFactory(object):
    """
    This protocol factory returns receiver instances used to spawn processes
    from a module. This implementation creates process class instances together
    with the receiver. This is a standard implementation that can be used
    in the code of every module containing a process. This factory also collects
    process declarations alongside.
    """
    implements(IProcessFactory)

    receiver = ProcessReceiver

    def __init__(self, pcls, name=None, args=None):
        self.process_class = pcls
        self.name = name or pcls.__name__
        self.args = args or {}

        if self.process_class and not IProcess.implementedBy(self.process_class):
            raise RuntimeError("Class does not implement IProcess")

        # Collecting the declare static class variable in a process class
        if pcls and hasattr(pcls, 'declare') and type(pcls.declare) is dict:
            procdec = pcls.declare.copy()
            procdec['class'] = pcls
            procname = pcls.declare.get('name', pcls.__name__)
            if procname in processes:
                raise RuntimeError('Process already declared: '+str(procname))
            processes[procname] = procdec

    def build(self, spawnargs=None, container=None):
        """
        Factory method to return a process instance from given arguments.
        """
        spawnargs = spawnargs or {}
        container = container or ioninit.container_instance

        #log.debug("ProcessFactory.build(name=%s, args=%s)" % (self.name,spawnArgs))

        # Create a process receiver
        procname = spawnargs.get('proc-name', self.name)
        procid = spawnargs.get('proc-id', self.name)
        spawnargs['proc-group'] = self.name

        # Instantiate the IProcess class
        process = self.process_class(spawnargs=spawnargs)

        return process

class ProcessInstantiator(object):
    """
    Instantiator for IProcess instances. Relies on an IProcessFactory
    to instantiate IProcess instances from modules.
    """

    idcount = 0

    @classmethod
    def create_process_id(cls, container=None):
        container = container or ioninit.container_instance
        cls.idcount += 1
        return Id(cls.idcount, container.id)

    @classmethod
    @defer.inlineCallbacks
    def spawn_from_module(cls, procmod, space, spawnargs=None, container=None):
        """
        @brief Factory method to spawn a BaseProcess instance from a Python module
        @param m A module (<type 'module'>)
        @param space container.MessageSpace instance
        @param spawnargs argument dict given to the factory on spawn
        @retval Deferred which fires with the instance id (container.Id)
        """
        spawnargs = spawnargs or {}
        container = container or ioninit.container_instance

        if not hasattr(procmod, 'factory'):
            raise RuntimeError("Must define factory in process module to spawn")

        if not IProcessFactory.providedBy(procmod.factory):
            raise RuntimeError("Process model factory must provide IProcessFactory")

        procid = ProcessInstantiator.create_process_id(container)
        spawnargs['proc-id'] = procid

        process = yield defer.maybeDeferred(procmod.factory.build, spawnargs)
        if not IProcess.providedBy(process):
            raise RuntimeError("ProcessFactory returned non-IProcess instance")

        # Give a callback to the process
        yield process.spawn()

        defer.returnValue(process)
