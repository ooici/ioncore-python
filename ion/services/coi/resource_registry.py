#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry.py
@author Michael Meisinger
@brief service for registering resources
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.services.base_service import BaseService, BaseServiceClient, RpcClient

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class ResourceRegistryService(BaseService):
    """Resource registry service interface
    """


# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = ResourceRegistryService(receiver)


