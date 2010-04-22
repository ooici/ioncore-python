#!/usr/bin/env python

"""
@file ion/services/cei/epu_controller.py
@author Alex Clemesha
@brief Evaluate data from Sensor Aggregator against policies and take compensation actions.
"""

import logging
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUControllerService(BaseService):
    """EPU Controller service interface
    """
    pass

# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = EPUControllerService(receiver)
