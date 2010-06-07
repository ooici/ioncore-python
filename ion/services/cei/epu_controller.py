#!/usr/bin/env python

"""
@file ion/services/cei/epu_controller.py
@author Alex Clemesha
@brief Evaluate data from Sensor Aggregator against policies and take compensation actions.
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService
from ion.core.base_process import ProtocolFactory

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUControllerService(BaseService):
    """EPU Controller service interface
    """
    declare = BaseService.service_declare(name='epu_controller', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        prov = yield self.get_scoped_name('system', 'provisioner')
        yield self.send(prov, 'cei_test', 'hello???!?')

    @defer.inlineCallbacks
    def op_sensor_aggregator_info(self, content, headers, msg):
        """
        Take in SensorAggregator info, and make Policy decisions (via the "Policy Engine").

        """
        logging.info("content:"+str(content))
        yield self.reply(msg, 'result', {'result':'policy-executed-success'}, {})        

    def op_cei_test(self, content, headers, msg):
        logging.info('EPU Controller: CEI test worked!!!!11! '+ content)

# Direct start of the service as a process with its default name
factory = ProtocolFactory(EPUControllerService)
