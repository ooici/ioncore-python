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
from ion.core.base_process import BaseProcess, ProtocolFactory
from ion.services.base_service import BaseService

class WorkerProcess(BaseService):
    """Worker process
    """
    # Declaration of service
    declare = BaseService.service_declare(name='worker', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        msg_name = self.spawn_args['service-name']
        scope = self.spawn_args['scope']
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
    def op_work(self, content, headers, msg):
        yield self._work(content)
        yield self.reply(msg, 'result', {'work-id':content['work-id']}, {})

    @defer.inlineCallbacks
    def _work(self,content):
        myid = self.proc_name + ":" + self.receiver.spawned.id.local
        workid = str(content['work-id'])
        waittime = float(content['work'])
        logging.info("worker="+myid+" job="+workid+" work="+str(waittime))
        yield pu.asleep(waittime)
        logging.info("worker="+myid+" job="+workid+" done at="+str(pu.currenttime_ms()))

class WorkerClient(BaseProcess):
    """Class for the client accessing the object store.
    """
    def __init__(self, *args):
        BaseProcess.__init__(self, *args)
        self.workresult = {}
        self.worker = {}

    def op_result(self, content, headers, msg):
        ts = pu.currenttime_ms()
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
        yield self.send(str(to),'work',{'work-id':workid,'work':work},{})

# Spawn of the process using the module name
factory = ProtocolFactory(WorkerProcess)
