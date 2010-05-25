
from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.services.cei.sensors import RabbitMQSensor 
import ion.util.procutils as pu

class ProvisionerSensorAggregatorInteractions(IonTestCase):
    """Testing interaction patterns between Provisioner and SensorAggregator.
    """

    #@defer.inlineCallbacks
    def setUp(self):
        pass

    #@defer.inlineCallbacks
    def tearDown(self):
        pass

    @defer.inlineCallbacks
    def test_rabbitmq_sensor(self):
        test_queue = "stocks"
        rabbitmq_sensor = RabbitMQSensor(test_queue)
        yield pu.asleep(6) #async wait




