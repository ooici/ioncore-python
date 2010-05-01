#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging, time
from twisted.internet import defer
from twisted.trial import unittest

from ion.core import bootstrap
from ion.core import base_process
from ion.test.iontest import IonTestCase

class WorkerTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._startMagnet()
        yield self._startCoreServices()


    @defer.inlineCallbacks
    def tearDown(self):
        ""
        yield self._stopMagnet()

    @defer.inlineCallbacks
    def _test_basic(self):
        messaging = {}
        
        workers = [
            {'name':'hello','module':'ion.services.hello_service','class':'HelloService'},
            {'name':'hello1','module':'ion.services.hello_service','class':'HelloService'},
            {'name':'hello2','module':'ion.services.hello_service','class':'HelloService'},
        ]
        
        yield bootstrap.bootstrap(messaging, workers)
 
     #@defer.inlineCallbacks
    def test_worker_queue(self):
        messaging = {'worker1':{'name_type':'worker', 'args':{'scope':'local'}}}
        
        workers = [
            {'name':'workerProc1','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'worker1','scope':'local'}},
            {'name':'workerProc2','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'worker1','scope':'local'}},
        ]
        
        yield bootstrap.bootstrap(messaging, workers)
        
        sup = yield base_process.procRegistry.get("bootstrap")
        logging.info("Supervisor: "+repr(sup))


    #@defer.inlineCallbacks
    def test_fanout(self):
        messaging = {'fanout1':{'name_type':'fanout', 'args':{'scope':'local'}}}

        workers = [
            {'name':'fanoutProc1','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'fanout1','scope':'local'}},
            {'name':'fanoutProc2','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'fanout1','scope':'local'}},
        ]

        yield bootstrap.bootstrap(messaging, workers)
        
        sup = yield base_process.procRegistry.get("bootstrap")
        logging.info("Supervisor: "+repr(sup))