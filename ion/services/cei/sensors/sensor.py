"""
'sensors.py'
------------  
    - "dedicated SensorProcess" (to work with 'dedicated SA'). 
    - start them by parametrization ('queue name to monitor', where to put samples).
 
How to discover "real" queue names?
-----------------------------------
    - who knows the Worker queues names? (the EPUController)
    - "EPUController is the one that creates the work Queue"
    - "scientists coming have to know a couple givens: [policy] and [Work queue].
 
Workflow
---------
    - write config/bootstrap-ctx for EPUController that specify "Work Types Names" (to be used to create WorkQueues, etc)
    - "1 EPUCtlr => 1'Work Type' => '1 WorkQueue' implies=> '1 SA for WorkQueue'"
    - IMPORTANT: Avoid bootstrap ordering OR The Controller starts the SA?
     
Test case for SensorAggregator
-------------------------------
    - take the existing "test_worker" (pull out important pieces from worker_test.py)
     
"""
#XXX All 'Sensors' are 'Process'. Should they live in a different dir that 'ion.services.cei'?

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


from twisted.internet.defer import inlineCallbacks, returnValue

from ion.core.base_process import BaseProcess


class SensorProcess(BaseProcess):
    """Base Sensor Process.

    Intended to be subclassed to target specific Sensor data.
    """

    sensor_client = None

    def plc_init(self):
        #TODO: pull LoopingCall into base class
        self.sensor_interval = self.spawn_args.get("sensor_interval", 2) # seconds per execution of 'sensor_loop'.
        self.start_immediately = self.spawn_args.get("start_immediately", True) #start sensor_loop right away.

