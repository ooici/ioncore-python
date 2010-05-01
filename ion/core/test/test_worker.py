#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging
from twisted.internet import defer
from twisted.trial import unittest

from ion.core import bootstrap
from ion.core.base_process import procRegistry
from ion.test.iontest import IonTestCase

class WorkerTest(IonTestCase):
    """Testing service classes of resource registry
    """

    #@defer.inlineCallbacks
    def setUp(self):
        IonTestCase.setUp(self)

    #@defer.inlineCallbacks
    def tearDown(self):
        IonTestCase.tearDown(self)

    @defer.inlineCallbacks
    def test_worker_queue(self):
        messaging = {'worker1':{'name_type':'worker', 'args':{'scope':'local'}}}
        
        workers = [
            {'name':'workerProc1','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'worker1','scope':'local'}},
            {'name':'workerProc2','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'worker1','scope':'local'}},
        ]
        
        yield self._startMagnet()
        yield bootstrap.bootstrap(messaging, workers)
        
        sup = yield procRegistry.get("bootstrap")
        logging.info("Supervisor: "+repr(sup))
        
        yield self._stopMagnet()
        

    def _test_fanout(self):
        workers = [
            {'name':'fanoutProc1','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'fanout1','scope':'local'}},
            {'name':'fanoutProc2','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'fanout1','scope':'local'}},
        ]
