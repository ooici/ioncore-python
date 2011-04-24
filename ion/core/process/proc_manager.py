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

from ion.core import ioninit
from ion.core.process import process
from ion.core.process.process import Process
from ion.core.process.process import IProcess, ProcessDesc, ProcessInstantiator
from ion.core.data.store import Store
from ion.util.state_object import BasicLifecycleObject
import ion.util.procutils as pu

class ProcessManager(BasicLifecycleObject):
    """
    Manager class for capability container process management.
    """

    def __init__(self, container):
        BasicLifecycleObject.__init__(self)
        self.container = container
        self.supervisor = None

        # TEMP: KVS pid (str) -> Process Instance
        self.process_registry = Store()

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
        if self.supervisor and self.supervisor._get_state() != 'TERMINATED':
            return self.supervisor.shutdown()
        else:
            return defer.succeed(None)

    def on_error(self, *args, **kwargs):
        raise RuntimeError("Illegal state change for ProcessManager")

    # API

    @defer.inlineCallbacks
    def spawn_processes(self, procs, sup=None):
        """
        Spawns a list of processes.
        @param procs  list of processes (as description dict) to start up
        @param sup  spawned Process instance acting as supervisor
        @retval Deferred -> Process instance
        """
        children = []
        for procDef in procs:
            child = ProcessDesc(**procDef)
            children.append(child)

        if sup == None:
            sup = yield self.create_supervisor()

        assert IProcess.providedBy(sup), "Parent must provide IProcess"
        assert sup._get_state() in ("READY", "ACTIVE"), "Illegal parent process state"

        log.info("Spawning %s child processes for sup=[%s]" % (len(children), sup.proc_name))
        for child in children:
            child_id = yield sup.spawn_child(child)

        #log.debug("process_ids: "+ str(process.procRegistry.kvs))

        defer.returnValue(sup)

    @defer.inlineCallbacks
    def spawn_process(self, procdesc, parent, node=None, activate=True):
        """
        @brief Spawns a process description with the initialized attributes.
        @param procdesc a ProcessDesc instance
        @param parent the process instance that should be set as supervisor
        @param node the container id where process should be spawned; None for local
        @retval Deferred -> Id with process id
        """
        if not parent:
            parent = yield self.create_supervisor()
            procdesc.sup_process = parent
        assert isinstance(procdesc, ProcessDesc), "procdesc must be ProcessDesc"
        assert IProcess.providedBy(parent), "parent must be IProcess"

        # The supervisor process id, system name and proc name are provided
        # as spawn args, in addition to any give arguments.
        spawnargs = procdesc.spawn_args or {}
        spawnargs['proc-name'] = procdesc.proc_name
        spawnargs['sup-id'] = parent.id.full
        spawnargs['sys-name'] = ioninit.sys_name

        log.info('Spawning name=%s on node=%s' % (procdesc.proc_name, procdesc.proc_node))
        if node:
            raise RuntimeError('Cannot spawn %s on node=%s (yet)' % (
                    procdesc.proc_class, procdesc.proc_node))
        else:
            process = yield self.spawn_process_local(
                    module=procdesc.proc_module,
                    spawnargs=spawnargs,
                    activate=activate)

            defer.returnValue(process.id)

    @defer.inlineCallbacks
    def spawn_process_local(self, module, space=None, spawnargs=None, activate=True):
        """
        @brief Spawn a process from a module, in the local container
        @param space is message space or None for container default space
        """
        if not space:
            space = ioninit.container_instance.exchange_manager.message_space
        if spawnargs == None:
            spawnargs = {}

        # Importing process module
        proc_mod = pu.get_module(module)

        process = yield ProcessInstantiator.spawn_from_module(
                module=proc_mod,
                spawnargs=spawnargs,
                container=self.container,
                activate=activate)
        yield self.register_local_process(process)

        defer.returnValue(process)

    # Sequence number of supervisors
    sup_seq = 0

    @defer.inlineCallbacks
    def create_supervisor(self):
        """
        Creates a supervisor process. There is only one root supervisor.
        @retval Deferred -> supervisor Process instance
        """
        if self.supervisor:
            defer.returnValue(self.supervisor)

        # Makes the boostrap a process
        log.info("Spawning supervisor")
        if self.sup_seq == 0:
            supname = "bootstrap"
        else:
            supname = "supervisor."+str(self.sup_seq)

        spawnargs = {'proc-name':supname}
        sup = process.factory.build(spawnargs)
        supId = yield sup.spawn()
        yield process.procRegistry.put(supname, str(supId))
        self.sup_seq += 1
        self.supervisor = sup
        defer.returnValue(sup)

    @defer.inlineCallbacks
    def activate_process(self, parent, pid):
        process = yield self.get_local_process(pid)
        if process:
            yield process.activate()

    @defer.inlineCallbacks
    def terminate_process(self, parent, pid):
        # Must send this request from the backend receiver
        (content, headers, msg) = yield parent.rpc_send(str(pid),
                                                'terminate', {}, {'quiet':True})
        defer.returnValue(headers)

    def register_local_process(self, process):
        """
        @retval Deferred
        """
        assert IProcess.providedBy(process), "process must be IProcess"
        return self.process_registry.put(process.id.full, process)

    def unregister_local_process(self, pid):
        """
        @retval Deferred
        """
        if not pid:
            return defer.succeed(None)
        else:
            return self.process_registry.remove(str(pid))

    def get_local_process(self, pid):
        """
        @retval Deferred -> IProcess instance
        """
        if not pid:
            return defer.succeed(None)
        else:
            return self.process_registry.get(str(pid))
