#!/usr/bin/env python

"""
@file ion/services/cei/epu_controller_decision_engine.py
@author Alex Clemesha
@brief Stateless decision engine that constantly evaluates sensor data and given policies/hueristics.

"""

import logging
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUControllerDecisionEngineService(BaseService):
    """EPU Controller Decision Engine service interface
    """
    pass

# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = EPUControllerDecisionEngineService(receiver)
