#!/usr/bin/env python

"""
@file ion/services/sa/instrument_registry.py
@author Michael Meisinger
@brief service for registering instruments and platforms
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class InstrumentRegistryService(BaseService):
    """Data acquisition service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='instrument_registry', version='0.1.0', dependencies=[])
 
    def op_define_instrument(self, content, headers, msg):
        """Service operation: Create or update an instrument registration
        """

    def op_define_agent(self, content, headers, msg):
        """Service operation: Create or update instrument or platform agent
        and register with an instrument or platform.
        """

    def op_register_agent_instance(self, content, headers, msg):
        """Service operation: .
        """

    def op_define_platform(self, content, headers, msg):
        """Service operation: Create or update a platform registration
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(InstrumentRegistryService)
