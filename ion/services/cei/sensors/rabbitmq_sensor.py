import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.services.cei.sensors.sensor import SensorProcess
from ion.core.process.process import ProcessFactory
from ion.services.cei import cei_events
from ion.services.cei.queuestat import QueueStatClient

class RabbitMQSensor(SensorProcess):
    """Obtain specific RabbitMQ data.

    Uses the queuestat service to communicate with RabbitMQ.
    """
    @defer.inlineCallbacks
    def plc_init(self):
        SensorProcess.plc_init(self)
        self.queue_name_work = self.get_scoped_name("system", 
                self.spawn_args["queue_name_work"])
        self.epu_controller = self.get_scoped_name("system", "epu_controller")
        
        self.queuestat_client = QueueStatClient(self)
        yield self.queuestat_client.watch_queue(self.queue_name_work, 
                str(self.id), 'stat')

    @defer.inlineCallbacks
    def op_stat(self, content, headers, msg):
        queuelen = content['queue_length']
        queuename = content['queue_name']
        extradict = {"queuelen": queuelen, "queue_name": queuename}
        cei_events.event("queue_sensor", "queuelen", log, extra=extradict)
        
        content = {"queue_id":queuename, "queuelen":queuelen}
        yield self.send(self.epu_controller, "sensor_info", content)

# Direct start of the service as a process with its default name
factory = ProcessFactory(RabbitMQSensor)
