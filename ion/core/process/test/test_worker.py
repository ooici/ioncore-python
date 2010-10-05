#!/usr/bin/env python

"""
@file ion/core/process/test_worker.py
@author Michael Meisinger
@brief test worker processes
"""
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.base_process import BaseProcess, ProcessFactory
from ion.core.cc.container import Container
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class WorkerTest(IonTestCase):
    """
    Testing worker processes
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_worker_queue(self):
        workers = [
            {'name':'workerProc1','module':'ion.core.process.worker','spawnargs':{'receiver-name':'worker1','scope':'system','receiver-type':'worker'}},
            {'name':'workerProc2','module':'ion.core.process.worker','spawnargs':{'receiver-name':'worker1','scope':'system','receiver-type':'worker'}},
        ]
        sup = yield self._spawn_processes(workers)
        log.info("Supervisor: "+repr(sup))

        wc = WorkerClient()
        wcId = yield self._spawn_process(wc)

        wq_name = ioninit.sys_name + ".worker1"
        for i in range(1,11):
            yield wc.submit_work(wq_name, i, 0.5)

        yield pu.asleep(7)
        log.info("Work results: %s" % (wc.workresult))
        log.info("Worker results: %s" % (wc.worker))

        sum = 0
        for w,v in wc.worker.items():
            sum += v
        self.assertEqual(sum, 10)

    @defer.inlineCallbacks
    def test_fanout(self):
        workers = [
            {'name':'fanoutProc1','module':'ion.core.process.worker','spawnargs':{'receiver-name':'fanout1','scope':'system','receiver-type':'fanout'}},
            {'name':'fanoutProc2','module':'ion.core.process.worker','spawnargs':{'receiver-name':'fanout1','scope':'system','receiver-type':'fanout'}},
        ]
        sup = yield self._spawn_processes(workers)
        log.info("Supervisor: "+repr(sup))

        wc = WorkerClient()
        wcId = yield self._spawn_process(wc)

        wq_name = ioninit.sys_name + ".fanout1"
        for i in range(1,6):
            yield wc.submit_work(wq_name, i, 0.5)

        yield pu.asleep(5)
        log.info("Work results: "+str(wc.workresult))
        log.info("Worker results: "+str(wc.worker))

        sum = 0
        for w,v in wc.worker.items():
            sum += v
        self.assertEqual(sum, 10)

class WorkerClient(BaseProcess):
    """
    Client for worker processes.
    """
    def __init__(self, *args, **kwargs):
        BaseProcess.__init__(self, *args, **kwargs)
        self.workresult = {}
        self.worker = {}

    def op_result(self, content, headers, msg):
        ts = pu.currenttime_ms()
        log.info("Work result received %s at %s " % (content, ts))
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
        yield self.send(to, 'work', {'work-id':workid,'work':work})
