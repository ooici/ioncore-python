#!/usr/bin/env python

import logging
from twisted.internet import defer
from magnet.container import Container
from magnet.spawnable import spawn

from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.services.cei.provisioner import ProvisionerService
from ion.services.cei.sensor_aggregator import SensorAggregatorService

class ControllerProvisionerSensorAggregatorInteractions(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_cei_test(self):
        messaging = {'cei':{'name_type':'worker', 'args':{'scope':'local'}}}
        procs = [
            {'name':'sensor_aggregator','module':'ion.services.cei.sensor_aggregator','class':'SensorAggregatorService'},
            {'name':'provisioner','module':'ion.services.cei.provisioner','class':'ProvisionerService'},
            {'name':'epu_controller','module':'ion.services.cei.epu_controller','class':'EPUControllerService'}
        ]
        yield self._declare_messaging(messaging)
        supervisor = yield self._spawn_processes(procs)

        yield pu.asleep(10) #async wait

