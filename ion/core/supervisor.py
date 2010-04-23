#!/usr/bin/env python

"""
@file ion/core/supervisor.py
@author Michael Meisinger
@brief base class for processes that supervise other processes
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver
from magnet.spawnable import spawn

from ion.core import ionconst as ic
import ion.util.procutils as pu
from ion.core.base_process import BaseProcess, RpcClient

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class Supervisor(BaseProcess):
    """
    Base class for a supervisor process
    """

    # Static definition of child processes
    childProcesses = []

    @defer.inlineCallbacks
    def spawnChildProcesses(self):
        logging.info("Spawning child processes")
        for child in self.childProcesses:
            yield child.spawnChild(self)

    @defer.inlineCallbacks
    def initChildProcesses(self):
        logging.info("Sending init message to all processes")
                
        for child in self.childProcesses:
            yield child.initChild()
            
    def event_failure(self):
        return

    
class ChildProcess(object):
    """
    Class that encapsulates attributes about a child process
    """
    def __init__(self, procMod, procClass=None, node=None):
        self.procModule = procMod
        self.procClass = procClass
        self.procNode = node
        self.procState = 'DEFINED'
        
    @defer.inlineCallbacks
    def spawnChild(self, supProc=None):
        self.supProcess = supProc
        if self.procNode == None:
            logging.info('Spawning '+self.procClass+' on node: local')

            # Importing service module
            logging.info('from ' + self.procModule + " import " + self.procClass)
            localmod = self.procModule.rpartition('.')[2]
            imports = []
            imports.append(localmod)
            if self.procClass != None:
                imports.append(self.procClass)
            proc_mod = __import__(self.procModule, globals(), locals(), imports)
            self.procModObj = proc_mod
        
            # Spawn instance of a process
            proc_id = yield spawn(proc_mod)
            self.procId = proc_id
            self.procState = 'SPAWNED'

            logging.info("Process "+self.procClass+" ID: "+str(proc_id))
        else:
            logging.error('Cannot spawn '+child.procClass+' on node '+str(child.node))
    
    @defer.inlineCallbacks
    def initChild(self):
        to = self.procId
        logging.info("Send init: " + str(to))
        self.rpc = RpcClient()
        yield self.rpc.attach()

        res = yield self.rpc.rpc_send(to, 'init', self._prepInitMsg(), {})
    
    def _prepInitMsg(self):
        return {'proc-name':self.procName, 'sup-id':self.supProcess.receiver.spawned.id.full}

# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = Supervisor(receiver)

@defer.inlineCallbacks
def test_sup():
    children = [
        ChildProcess('ion.services.coi.resource_registry','ResourceRegistryService',None),
        ChildProcess('ion.services.coi.service_registry','ServiceRegistryService',None),
    ]
    instance.childProcesses = children
    sup_id = yield spawn(receiver)
    
    yield instance.spawnChildProcesses()
    
    logging.info('Spawning completed')

"""
from ion.core import supervisor as s
s.test_sup()
"""