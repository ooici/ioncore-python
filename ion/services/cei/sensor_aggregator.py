#!/usr/bin/env python

"""
@file ion/services/cei/sensor_aggregator.py
@author Alex Clemesha
@brief Obtains data about exchange points, EPU workers, and operational unit data/statuses.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.core.process.service_process import ServiceProcess
from ion.core.process.process import ProcessFactory

class SensorAggregatorService(ServiceProcess):
    """SensorAggregator service interface
    """
    declare = ServiceProcess.service_declare(name='sensor_aggregator', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def op_node_status(self, content, headers, msg):
        epu_controller = yield self.get_scoped_name('system', 'epu_controller')
        log.info('SensorAggregatorService.node_status called.')
        yield self.send(epu_controller, 'sensor_info', content)

# Direct start of the service as a process with its default name
factory = ProcessFactory(SensorAggregatorService)
