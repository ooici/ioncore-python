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
    def op_provisioner_iaas_info(self, content, headers, msg):
        logging.info("op_provisioner_iaas_info  content:"+str(content))
        # do "Aggregation", etc here
        yield self.reply(msg, 'result', {'result':'receipt-taking-success'}, {})        

# Direct start of the service as a process with its default name
factory = ProtocolFactory(SensorAggregatorService)
