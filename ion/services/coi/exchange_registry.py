#!/usr/bin/env python

"""
@file ion/services/coi/exchange_registry.py
@author Michael Meisinger
@brief service for registering exchange names
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class ExchangeRegistryService(BaseService):
    """Exchange registry service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='exchange_registry', version='0.1.0', dependencies=[])

    def op_define_exchange_space(self, content, headers, msg):
        """Service operation: .
        """

    def op_join_exchange_space(self, content, headers, msg):
        """Service operation: 
        """

    def op_leave_exchange_space(self, content, headers, msg):
        """Service operation: 
        """

    def op_define_exchange_name(self, content, headers, msg):
        """Service operation: Create or update a name in an exchange space
        """
        
    def op_add_broker(self, content, headers, msg):
        """Service operation: Add a message broker to the federation
        """

    def op_remove_broker(self, content, headers, msg):
        """Service operation: Remove a message broker from the federatione
        """

# Spawn of the process using the module name
factory = ProtocolFactory(ExchangeRegistryService)

