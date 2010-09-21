#!/usr/bin/env python

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.core.cc.container import Container
from ion.core.cc.spawnable import spawn

from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.services.cei.test.mockloop.provisioner import MockLoopProvisionerService

class MockLoop(IonTestCase):
    """Testing interaction patterns between all CEI components.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_mockloopprovisioner(self):
        messaging = {'cei':{'name_type':'worker', 'args':{'scope':'local'}}}
        procs = [
            {'name':'sensor_aggregator','module':'ion.services.cei.sensor_aggregator','class':'SensorAggregatorService'},
            {'name':'epu_controller','module':'ion.services.cei.epu_controller','class':'EPUControllerService'},
            {'name':'provisioner','module':'ion.services.cei.test.mockloop.provisioner','class':'MockLoopProvisionerService'}
        ]
        yield self._declare_messaging(messaging)
        supervisor = yield self._spawn_processes(procs)

        aggregatorId = yield self.procRegistry.get("sensor_aggregator")
        controllerId = yield self.procRegistry.get("epu_controller")
        provisionerId = yield self.procRegistry.get("provisioner")
        log.info("aggregatorId: "+repr(aggregatorId) + ", " + "provisionerId: "+repr(provisionerId) + ", " + "controllerId: "+repr(controllerId))
        
        testmsg = {
                    'operation':'start',
                    'deployable_type':'extraction-service-dt-5124', 
                     'launch_id':'fa6baaaf-b4a3-4969-b10f-99f87b3117cd',
                    'instances' : {
                         'head-node' : {
                            'id':['e61f0c0e-781e-4681-adb2-7dada1cf31a4'],
                            'allocation' : 'x-large',
                            'site':'ec2-west',
                            'data' : {}
                         },
                        'worker-node' : { 
                            'id': ['5fee8009-67db-4efc-8f35-4cee5d65c917', '228a0ea6-e1eb-4634-b57f-1f4162a89f63'],
                            'allocation' : 'small',
                            'site':'ec2-west',
                            'data' : {}
                         },
                    },
                    'subscribers' : ['my_sensor_aggregator']
                }

        yield pu.asleep(5)
        
        yield supervisor.send(provisionerId, "provision", testmsg)
        
        yield pu.asleep(1000000) # "forever"
        

