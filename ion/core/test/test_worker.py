#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging
from twisted.internet import defer
from magnet.container import Container
from magnet.spawnable import spawn

from ion.core.worker import WorkerClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class WorkerTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_worker_queue(self):
        messaging = {'worker1':{'name_type':'worker', 'args':{'scope':'local'}}}

        workers = [
            {'name':'workerProc1','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'worker1','scope':'system'}},
            {'name':'workerProc2','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'worker1','scope':'system'}},
        ]

        yield self._declare_messaging(messaging)
        yield self._spawn_processes(workers)

        sup = yield self._get_procid("bootstrap")
        logging.info("Supervisor: "+repr(sup))

        wc = WorkerClient()
        wcId = yield wc.spawn()
        yield wc.init()

        wq_name = Container.id + ".worker1"
        for i in range(1,11):
            yield wc.submit_work(wq_name, i, 0.5)

        yield pu.asleep(7)
        logging.info("Work results: "+str(wc.workresult))
        logging.info("Worker results: "+str(wc.worker))

        sum = 0
        for w,v in wc.worker.items():
            sum += v
        self.assertEqual(sum, 10)

    @defer.inlineCallbacks
    def test_fanout(self):
        messaging = {'fanout1':{'name_type':'fanout', 'args':{'scope':'local'}}}

        workers = [
            {'name':'fanoutProc1','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'fanout1','scope':'local'}},
            {'name':'fanoutProc2','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'fanout1','scope':'local'}},
        ]

        yield self._declare_messaging(messaging)
        yield self._spawn_processes(workers)

        sup = yield self._get_procid("bootstrap")
        logging.info("Supervisor: "+repr(sup))

        wc = WorkerClient()
        wcId = yield wc.spawn()
        yield wc.init()

        wq_name = Container.id + ".fanout1"
        for i in range(1,6):
            yield wc.submit_work(wq_name, i, 0.5)

        yield pu.asleep(5)
        logging.info("Work results: "+str(wc.workresult))
        logging.info("Worker results: "+str(wc.worker))

        sum = 0
        for w,v in wc.worker.items():
            sum += v
        self.assertEqual(sum, 10)
