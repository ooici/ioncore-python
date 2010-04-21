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

    @defer.inlineCallbacks
    def spawnChildProcesses(self):
        logging.info("Spawning child processes")
        for child in self.childProcesses:
            yield child.spawnChild()

    @defer.inlineCallbacks
    def initChildProcesses(self):
        logging.info("Sending init message to all processes")
        for child in self.childProcesses:
            to = child.procId
            print "Send to: ",to
            yield self.send_message(to, 'init', self._prepInitMsg(child), {})
            
    def event_failure(self):
        return
    
    def _prepInitMsg(self, child):
        return {'proc-name':child.procName, 'sup-id':self.receiver.spawned.id.full}
    
class ChildProcess(object):
    """
    Class that encapsulates attributes about a child process
    """
    def __init__(self, procMod, procClass, node=None):
        self.procModule = procMod
        self.procClass = procClass
        self.procNode = node
        self.procState = 'DEFINED'
        
    @defer.inlineCallbacks
    def spawnChild(self):
        if self.procNode == None:
            logging.info('Spawning '+self.procClass+' on node: local')

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
    
    yield instance.spawnChildProcesses()
    
    logging.info('Spawning completed')

"""
from ion.core import supervisor as s
s.test_sup()
"""