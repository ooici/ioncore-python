#!/usr/bin/env python

"""
@file ion/services/coi/service_registry.py
@author Michael Meisinger
@brief service for registering service (types and instances).
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class ServiceRegistryService(BaseService):
    """Service registry service interface
    """

    def op_register_service(self, content, headers, msg):
        pass

    def op_get_service_spec(self, content, headers, msg):
        pass

    def op_register_instance(self, content, headers, msg):
        pass

    def op_get_instance(self, content, headers, msg):
        pass
    
# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = ServiceRegistryService(receiver)
