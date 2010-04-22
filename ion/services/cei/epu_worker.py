#!/usr/bin/env python

"""
@file ion/services/cei/epu_worker.py
@author Alex Clemesha
@brief Consumes work messages and performs work described in each message.
"""

import logging
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUWorkerService(BaseService):
    """EPU Worker service interface
    """
    pass

# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = EPUWorkerService(receiver)
