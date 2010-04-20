#!/usr/bin/env python

"""
@file ion/core/base_process.py
@author Michael Meisinger
@author Stephen Pasco
@brief base class for all processes within Magnet
"""

import logging

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import ion.util.procutils as pu

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class BaseProcess(object):
    """
    This is the abstract superclass for all processes.
    """

    store = Store()
    receiver = None
    procName = __name__
    procId = None
    
    def __init__(self, procName=__name__):
        """Constructor using a given name for the spawnable receiver.
        """
        logging.debug('BaseProcess.__init__('+procName+')')

        self.procName = procName
        self.receiver = Receiver(procName)
        logging.info('Created receiver: '+str(self.receiver)+" for "+procName)

    @defer.inlineCallbacks
    def plc_start(self):
        """Performs the start of the process. Creates the actual spawned
        process.
        
        @return deferred
        """
        logging.info('Process '+self.procName+' plc_start()')

        self.procId = yield spawn(self.receiver)
        logging.info('Spawned process with id='+str(self.procId))
        

    def op_noop_catch(self, content, headers, msg):
        """The method called if operation is not defined
        """
        logging.info('Catch message')

            
    def receive(self, content, msg):
        pu.dispatch_message(content, msg, self)
    
    def send_message(to,operation,content,headers):
        """Send a message via the processes receiver to a
        """
        src = procId
        pu.send_message(receiver,src,to,operation,content,headers)
