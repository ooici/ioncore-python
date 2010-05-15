#!/usr/bin/env python

"""
@file ion/services/cei/test/test_provisioner_sensor_aggregator_interactions.py
@author Alex Clemesha
@brief Test interaction patterns between Provisioner and Sensor Aggregator.
"""

import logging
from twisted.internet import defer
from magnet.container import Container
from magnet.spawnable import spawn

from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.services.cei.provisioner import ProvisionerService
from ion.services.cei.sensor_aggregator import SensorAggregatorService

class ProvisionerSensorAggregatorInteractions(IonTestCase):
    """Testing interaction patterns between Provisioner and SensorAggregator.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_provisioner_to_controller(self):
        messaging = {'cei':{'name_type':'worker', 'args':{'scope':'local'}}}
        procs = [
            {'name':'sensor_aggregator','module':'ion.services.cei.sensor_aggregator','class':'SensorAggregatorService'},
            {'name':'provisioner','module':'ion.services.cei.provisioner','class':'ProvisionerService'}
        ]
        yield self._declare_messaging(messaging)
        supervisor = yield self._spawn_processes(procs)

        saId = yield self.procRegistry.get("sensor_aggregator")
        pId = yield self.procRegistry.get("provisioner")
        logging.info("saId: "+repr(saId) + " " + "pId: "+repr(pId))

        # The below should "kick off" more complex test-able behavior:
        yield supervisor.send(pId, "provision", {"action":"start-sending-iaas-messages"})
        #TODO: add a Counter to Prov, and 'test' counter. (this is a testing hack)

        yield pu.asleep(5) #async wait

