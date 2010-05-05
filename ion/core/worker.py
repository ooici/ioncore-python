#!/usr/bin/env python

"""
@file ion/services/coi/exchange_registry.py
@author Michael Meisinger
@brief service for registering exchange names
"""

import logging, time
from twisted.internet import defer
from magnet.spawnable import Receiver
from magnet.spawnable import spawn
from magnet.spawnable import Container
from magnet import spawnable

import ion.util.procutils as pu
from ion.core.base_process import BaseProcess, ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class WorkerProcess(BaseService):
    """Worker process
    """

    @defer.inlineCallbacks
    def slc_init(self):
        msg_name = self.spawnArgs['service-name']
        scope = self.spawnArgs['scope']
        logging.info("slc_init name received:"+msg_name)
        msg_name1 = self.get_scoped_name(scope, msg_name)
        logging.info("slc_init name used:"+msg_name1)
        workReceiver = Receiver(__name__, msg_name1)
        self.workReceiver = workReceiver
        self.workReceiver.handle(self.receive)

        logging.info("slc_init worker receiver spawning")
        id = yield spawn(workReceiver)
        logging.info("slc_init worker receiver spawned:"+str(id))
    
    @defer.inlineCallbacks
    def op_workx(self, content, headers, msg):
        yield self._work(content)

    @defer.inlineCallbacks
    def op_work(self, content, headers, msg):
        yield self._work(content)
        yield self.reply_message(msg, 'result', {'work-id':content['work-id']}, {})        

    @defer.inlineCallbacks
    def _work(self,content):
        myid = self.procName + ":" + self.receiver.spawned.id.local
        workid = str(content['work-id'])
        waittime = int(content['work'])
        logging.info("worker="+myid+" job="+workid+" work="+str(waittime))
        yield pu.asleep(waittime)
        logging.info("worker="+myid+" job="+workid+" done at="+str(time.clock()))

class WorkerClient(BaseProcess):
    """Class for the client accessing the object store.
    """
    def __init__(self, *args):
        BaseProcess.__init__(self, *args)
        self.workresult = {}
        self.worker = {}

    def op_result(self, content, headers, msg):
        ts = time.clock()
        logging.info("Work result received "+str(content)+" at "+str(ts))
        workid = content['work-id']
        worker = headers['sender']
        self.workresult[workid] = ts
        if worker in self.worker:
            wcnt = self.worker[worker] + 1
        else:
            wcnt = 1
        self.worker[worker] = wcnt

    @defer.inlineCallbacks
    def submit_work(self, to, workid, work):
        yield self.send_message(str(to),'work',{'work-id':workid,'work':work},{})

# Spawn of the process using the module name
factory = ProtocolFactory(WorkerProcess)
