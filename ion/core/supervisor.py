#!/usr/bin/env python

"""
@file ion/core/supervisor.py
@author Michael Meisinger
@brief base class for processes that supervise other processes
"""

import logging
from twisted.internet import defer
from magnet.spawnable import spawn

from ion.core.base_process import BaseProcess, ProtocolFactory
import ion.util.procutils as pu

class Supervisor(BaseProcess):
    """
    Base class for a supervisor process. A supervisor is a process with the
    purpose to spawn and monitor child processes.
    """
    def set_child_processes(self, childprocs):
        self.childProcesses = childprocs

    @defer.inlineCallbacks
    def spawn_child_processes(self):
        logging.info("Spawning child processes")
        for child in self.childProcesses:
            yield child.spawn_child(self)

    @defer.inlineCallbacks
    def init_child_processes(self):
        logging.info("Sending init message to all processes")

        for child in self.childProcesses:
            yield child.init_child()

        logging.info("All processes initialized")

    def event_failure(self):
        return


class ChildProcess(object):
    """
    Class that encapsulates attributes about a child process and can spawn
    child processes.
    """
    def __init__(self, procMod, procClass=None, node=None, spawnArgs=None):
        self.procModule = procMod
        self.procClass = procClass
        self.procNode = node
        self.spawnArgs = spawnArgs
        self.procState = 'DEFINED'

    @defer.inlineCallbacks
    def spawn_child(self, supProc=None):
        self.supProcess = supProc
        if self.procNode == None:
            logging.info('Spawning name=%s node=%s' % (self.procName, self.procNode))

            # Importing service module
            proc_mod = pu.get_module(self.procModule)
            self.procModObj = proc_mod

            # Spawn instance of a process
            # During spawn, the supervisor process id, system name and proc name
            # get provided as spawn args, in addition to any give spawn args.
            spawnargs = {'proc-name':self.procName,
                         'sup-id':self.supProcess.receiver.spawned.id.full,
                         'sys-name':self.supProcess.sysName}
            if self.spawnArgs:
                spawnargs.update(self.spawnArgs)
            #logging.debug("spawn(%s, args=%s)" % (self.procModule, spawnargs))
            proc_id = yield spawn(proc_mod, None, spawnargs)
            self.procId = proc_id
            self.procState = 'SPAWNED'

            #logging.info("Process "+self.procClass+" ID: "+str(proc_id))
        else:
            logging.error('Cannot spawn '+self.procClass+' on node='+str(self.procNode))

    @defer.inlineCallbacks
    def init_child(self):
        (content, headers, msg) = yield self.supProcess.rpc_send(self.procId, 'init', {}, {'quiet':True})

# Spawn of the process using the module name
factory = ProtocolFactory(Supervisor)

"""
from ion.core import supervisor as s
s.test_sup()
"""
