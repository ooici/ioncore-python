#!/usr/bin/env python

"""
@file ion/services/sa/instrument_registry.py
@author Michael Meisinger
@brief service for registering instruments and platforms
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class InstrumentRegistryService(BaseService):
    """Data acquisition service interface
    """
 
    def op_define_instrument(self, content, headers, msg):
        """Service operation: Create or update an instrument registration
        """

    def op_define_agent(self, content, headers, msg):
        """Service operation: Create or update instrument or platform agent
        and register with an instrument or platform.
        """

    def op_define_platform(self, content, headers, msg):
        """Service operation: Create or update a platform registration
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(InstrumentRegistryService)
