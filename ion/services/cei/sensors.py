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

import logging

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import LoopingCall

from ion.core.base_process import BaseProcess


class SensorProcess(BaseProcess):
    """Base Sensor Process.

    Intended to be subclassed to target specific Sensor data.
    """

    sensor_client = None

    def __init__(self, sensor_interval=2, start_immediately=True):
        self.sensor_interval = sensor_interval # seconds per execution of 'sensor_loop'.
        self.start_immediately = start_immediately #start sensor_loop right away.


class RabbitMQSensor(SensorProcess):
    """Obtain specific RabbitMQ data.

    Uses the 'txrabbitmq' library to communicate with RabbitMQ.
    """

    def __init__(self, queue_name, **kwargs):
        SensorProcess.__init__(self, **kwargs)
        self.queue_name = queue_name #queue to monitor
        self.sensor_client = self._create_sensor_client()
        self.sensor_loop = LoopingCall(self.messages_in_queue, self.queue_name)
        if self.start_immediately:
            self.sensor_loop.start(self.sensor_interval)

    def _create_sensor_client(self, erlang_cookie=None, nodename="txrabbitmq"):
        #XXX Move the below into the 'txrabbitmq' library
        from txrabbitmq.service import RabbitMQControlService
        from twotp.node import Process, readCookie, buildNodeName
        cookie = readCookie() #TODO: allow passing 'erlang_cookie'
        nodeName = buildNodeName(nodename)
        process = Process(nodeName, cookie)
        return RabbitMQControlService(process)

    @inlineCallbacks
    def messages_in_queue(self, queue_name):
        """
        Returns the number of existing messages in queue 'queue_name'.
        Returns -1 if 'queue_name' does not exist.
        """
        allqueues = yield self.sensor_client.list_queues()
        logging.info("=== messages_in_queue ===")
        for q in allqueues["result"]:
            if q[0] == queue_name:
                msgs = q[1]["messages"]
                logging.info("in queue '%s' there are '%s' messages"% (queue_name, msgs))
                returnValue(msgs)
        returnValue(-1) #'queue_name' was not found.

