import logging

from ion.services.cei.sensors.sensor import SensorProcess

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import LoopingCall

from ion.core.base_process import ProtocolFactory

class RabbitMQSensor(SensorProcess):
    """Obtain specific RabbitMQ data.

    Uses the 'txrabbitmq' library to communicate with RabbitMQ.
    """

    def plc_init(self):
        SensorProcess.plc_init(self)
        self.queue_name_work = self.spawn_args["queue_name_work"]
        self.queue_name_events = self.spawn_args["queue_name_events"]
        self.sensor_client = self._create_sensor_client()
        self.sensor_loop = LoopingCall(self.messages_in_queue)
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
    def op_stop(self, content, headers, msg):
        """Stop the LoopingCall"""
        self.sensor_loop.stop()
        yield self.reply_ok(msg)

    @inlineCallbacks
    def messages_in_queue(self):
        """
        Returns the number of existing messages in queue 'queue_name'.
        Returns -1 if 'queue_name' does not exist.
        """
        allqueues = yield self.sensor_client.list_queues()
        logging.info("=== messages_in_queue ===")
        for q in allqueues["result"]:
            if q[0] == self.queue_name_work:
                messages = q[1]["messages"]
                logging.info("in queue '%s' there are '%s' messages"% (self.queue_name_work, messages))
                content = {"queue_name_work":self.queue_name_work, "messages":messages}
                yield self.send(self.queue_name_events, "event", content)

# Direct start of the service as a process with its default name
factory = ProtocolFactory(RabbitMQSensor)
