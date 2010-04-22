#!/usr/bin/env python

"""
@file ion/services/cei/dtrs.py
@author Alex Clemesha
@brief Deployable Type Registry Service. Used to look up Deployable type data/metadata.
"""

import logging
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class DeployableTypeRegistryService(BaseService):
    """Deployable Type Registry service interface
    """
    pass


# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = DeployableTypeRegistryService(receiver)
