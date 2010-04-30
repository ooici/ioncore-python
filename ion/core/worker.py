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
from magnet.spawnable import ProtocolFactory

import ion.util.procutils as pu
from ion.core.base_process import RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)


class WorkerProcess(BaseService):
    """Worker process
    """

    @defer.inlineCallbacks
    def slc_init(self):
        workReceiver = Receiver(__name__, 'worker1')
        self.workReceiver = workReceiver
        id = yield spawn(workReceiver)
    
    def op_hello(self, content, headers, msg):
        logging.info('op_hello: '+str(content)+' id='+self.receiver.id.full)
        
    @defer.inlineCallbacks
    def startWorker(self):
        work_id = yield spawn(self.workReceiver)

