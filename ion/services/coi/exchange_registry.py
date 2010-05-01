#!/usr/bin/env python

"""
@file ion/services/coi/exchange_registry.py
@author Michael Meisinger
@brief service for registering exchange names
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class ExchangeRegistryService(BaseService):
    """Exchange registry service interface
    """
    
# Spawn of the process using the module name
factory = ProtocolFactory(ExchangeRegistryService)

