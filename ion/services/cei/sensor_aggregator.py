#!/usr/bin/env python

"""
@file ion/services/cei/sensor_aggregator.py
@author Alex Clemesha
@brief Obtains data about exchange points, EPU workers, and operational unit data/statuses.
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService
from ion.core.base_process import ProtocolFactory

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class SensorAggregatorService(BaseService):
    """SensorAggregator service interface
    """
    declare = BaseService.service_declare(name='sensor_aggregator', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def op_node_status(self, content, headers, msg):
        epu_controller = yield self.get_scoped_name('system', 'epu_controller')
        logging.info('SensorAggregatorService.node_status called.')
        yield self.send(epu_controller, 'sensor_info', content)

# Direct start of the service as a process with its default name
factory = ProtocolFactory(SensorAggregatorService)
