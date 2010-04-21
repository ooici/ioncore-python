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

import ion.util.procutils as pu
from ion.core.base_process import BaseProcess

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class Supervisor(BaseProcess):
    """
    Base class for a supervisor process
    """

    # Static definition of child processes
    childProcesses = []

    def spawnChildProcesses(self):
        for child in self.childProcesses:
            child.spawnChild()

    def event_failure(self):
        return
    
class ChildProcess(object):
    
    def __init__(self, procMod, procClass, node=None):
        self.procModule = procMod
        self.procClass = procClass
        self.procNode = node
        self.procState = 'DEFINED'
        
    @defer.inlineCallbacks
    def spawnChild(self):
        if self.procNode == None:
            logging.info('Spawning '+self.procClass+' on local node')

            # Importing service module
            logging.info('from ' + self.procModule + " import " + self.procClass)
            localmod = self.procModule.rpartition('.')[2]
            proc_mod = __import__(self.procModule, globals(), locals(), [localmod,self.procClass])
            self.procModObj = proc_mod
        
            # Spawn instance of a process
            proc_id = yield spawn(proc_mod)
            self.procId = proc_id
            self.procState = 'SPAWNED'

            logging.info("Process "+self.procClass+" ID: "+str(proc_id))
        else:
            logging.error('Cannot spawn '+child.procClass+' on node '+str(child.node))

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
    
    instance.spawnChildProcesses()
    logging.info('Spawning completed')

"""
from ion.core import supervisor as s
"""