#!/usr/bin/env python

"""
@file ion/services/cei/sensor_aggregator.py
@author Alex Clemesha
@brief Obtains data about exchange points, EPU workers, and operational unit data/statuses.
"""

import logging
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class SensorAggregatorService(BaseService):
    """SensorAggregator Service interface
    """
    pass


# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = SensorAggregatorService(receiver)
