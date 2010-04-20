#!/usr/bin/env python

"""
@file ion/services/coi/service_registry.py
@author Michael Meisinger
@brief service for registering service (types and instances).
"""

import logging
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import ion.util.procutils as pu
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.base_svcproc import BaseServiceProcess

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class ServiceRegistryService(BaseService):
    """Service registry service interface
    """
    
    def __init__(self):
        BaseService.__init__(self)
        logging.info('ServiceRegistryService.__init__()')

        
# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = ServiceRegistryService()

def receive(content, msg):
    pu.dispatch_message(content, msg, instance)

receiver.handle(receive)
