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
from magnet.spawnable import Container
from magnet import spawnable

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class WorkerProcess(BaseService):
    """Worker process
    """

    @defer.inlineCallbacks
    def slc_init(self):
        msg_name = self.spawnArgs['service-name']
        logging.info("slc_init name received:"+msg_name)
        msg_name1 = Container.id + "." + msg_name
        logging.info("slc_init name used:"+msg_name1)
        workReceiver = Receiver(__name__, msg_name1)
        self.workReceiver = workReceiver
        id = yield spawn(workReceiver)
    
    def op_hello(self, content, headers, msg):
        logging.info('op_hello: '+str(content)+' id='+self.receiver.id.full)
        
    @defer.inlineCallbacks
    def startWorker(self):
        work_id = yield spawn(self.workReceiver)

factory = ProtocolFactory(WorkerProcess)
