#!/usr/bin/env python

"""
@file ion/core/process/worker.py
@author Michael Meisinger
@brief base class for a worker process
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.base_process import BaseProcess, ProcessFactory
from ion.core.messaging.receiver import Receiver, WorkerReceiver, FanoutReceiver
from ion.services.base_service import BaseService
import ion.util.procutils as pu

class WorkerProcess(BaseProcess):
    """
    Worker process
    """
    @defer.inlineCallbacks
    def plc_init(self):
        msg_name = str(self.spawn_args['receiver-name'])
        rec_type = str(self.spawn_args['receiver-type'])
        scope = str(self.spawn_args['scope'])

        if rec_type == 'worker':
            self.workReceiver = WorkerReceiver(
                label=__name__,
                name=msg_name,
                scope=scope,
                handler=self.receive)
        elif rec_type == 'fanout':
            self.workReceiver = FanoutReceiver(
                label=__name__,
                name=msg_name,
                scope=scope,
                handler=self.receive)
        else:
            raise RuntimeError("Unknown receiver-type: "+str(rec_type))

        yield self.workReceiver.attach()

    @defer.inlineCallbacks
    def op_work(self, content, headers, msg):
        yield self._work(content)
        yield self.reply_ok(msg, {'work-id':content['work-id']})

    @defer.inlineCallbacks
    def _work(self,content):
        myid = self.proc_name + ":" + self.id.local
        workid = str(content['work-id'])
        waittime = float(content['work'])
        log.info("worker="+myid+" job="+workid+" work="+str(waittime))
        yield pu.asleep(waittime)
        log.info("worker="+myid+" job="+workid+" done at="+str(pu.currenttime_ms()))

# Spawn of the process using the module name
factory = ProcessFactory(WorkerProcess)
