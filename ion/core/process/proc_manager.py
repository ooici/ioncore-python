#!/usr/bin/env python

"""
@file ion/core/process/proc_manager.py
@author Michael Meisinger
@brief Process Manager for capability container
"""

import types

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit, base_process
from ion.core.base_process import BaseProcess
from ion.core.process.process import ProcessDesc, ProcessInstantiator
from ion.util.state_object import BasicLifecycleObject
import ion.util.procutils as pu

class ProcessManager(BasicLifecycleObject):
    """
    Manager class for capability container process management.
    """

    def __init__(self, container):
        BasicLifecycleObject.__init__(self)
        self.container = container

    # Life cycle

    def on_initialize(self, config, *args, **kwargs):
        """
        """
        self.config = config
        return defer.succeed(None)

    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        return defer.succeed(None)

    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        return defer.succeed(None)

    def on_error(self, *args, **kwargs):
        raise RuntimeError("Illegal state change for ProcessManager")

    # API

    @defer.inlineCallbacks
    def spawn_child(self, childproc, init=True):
        """
        Spawns a process described by the ProcessDesc instance as child of this
        process instance. An init message is sent depending on flag.
        @param childproc  ProcessDesc instance with attributes
        @param init  flag determining whether an init message should be sent
        @retval process id of the child process
        """
        assert isinstance(childproc, ProcessDesc)
        assert not childproc in self.child_procs
        self.child_procs.append(childproc)
        child_id = yield childproc.spawn(self)
        yield procRegistry.put(str(childproc.proc_name), str(child_id))
        if init:
            yield childproc.init()
        defer.returnValue(child_id)

    def spawn_process(self, module, space=None, spawnargs=None):
        """
        @brief Spawn a process from a module.
        @param space is message space or None for container default space

        Spawn uses a function as an entry point for running a module
        """
        assert type(module) is types.ModuleType, "Can only spawn from a module"
        if not space:
            space = ioninit.container_instance.message_space
        if spawnargs == None:
            spawnargs = {}

        return ProcessInstantiator.spawn_from_module(module, space, spawnargs)

    @defer.inlineCallbacks
    def spawn_processes(self, procs, sup=None):
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
            sup = yield self.create_supervisor()

        log.info("Spawning child processes")
        for child in children:
            child_id = yield sup.spawn_child(child)

        #log.debug("process_ids: "+ str(base_process.procRegistry.kvs))

        defer.returnValue(sup)

    # Sequence number of supervisors
    sup_seq = 0

    @defer.inlineCallbacks
    def create_supervisor(self):
        """
        Creates a supervisor process
        @retval Deferred, for supervisor BaseProcess instance
        """
        global sup_seq
        # Makes the boostrap a process
        log.info("Spawning supervisor")
        if sup_seq == 0:
            supname = "bootstrap"
        else:
            supname = "supervisor."+str(self.sup_seq)
        suprec = base_process.factory.build({'proc-name':supname})
        sup = suprec.process
        sup.receiver.group = supname
        supId = yield sup.spawn()
        yield base_process.procRegistry.put(supname, str(supId))
        sup_seq += 1
        defer.returnValue(sup)
