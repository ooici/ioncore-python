#!/usr/bin/env python

"""
@file ion/services/coi/exchange_registry.py
@author Michael Meisinger
@brief service for registering exchange names
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver
from magnet.spawnable import spawn

import ion.util.procutils as pu
from ion.core.base_process import RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class WorkerProcess(BaseService):
    """Worker process
    """
    
    def op_hello(self, content, headers, msg):
        logging.info('op_hello: '+str(content)+' id='+self.receiver.id.full)
        
    @defer.inlineCallbacks
    def startWorker(self):
        work_id = yield spawn(self.workReceiver)

# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
workReceiver = Receiver(__name__, 'worker1')

instance = WorkerProcess(receiver)
instance.workReceiver = workReceiver
instance.startWorker()

def factory(name=__name__, args={}):
    receiver = Receiver(__name__)
    return receiver
