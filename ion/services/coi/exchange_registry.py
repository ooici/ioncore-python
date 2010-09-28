#!/usr/bin/env python


"""
@file ion/services/coi/exchange/exchange_registry.py
@author Brian Fox
@brief provides a registry service for exchange spaces and exchange points
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.data.datastore import registry

from ion.core import ioninit
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

CONF = ioninit.config(__name__)


class ExchangeRegistryService(registry.BaseRegistryService):
    """
    @brief Skeleton exchange registry
    @todo flesh out
    """

    # Declaration of service
    declare = BaseService.service_declare(name='exchange_registry', version='0.1.0', dependencies=[])

    def op_define_exchange_space(self, content, headers, msg):
        pass
    
    def op_join_exchange_space(self, content, headers, msg):
        pass
    
    def op_leave_exchange_space(self, content, headers, msg):
        pass
    
    def op_define_exchange_name(self, content, headers, msg):
        """Service operation: Create or update a name in an exchange space
        """
        pass
    
    def op_add_broker(self, content, headers, msg):
        """Service operation: Add a message broker to the federation
        """
        pass
    
    def op_remove_broker(self, content, headers, msg):
        pass


class ExchangeRegistryClient(registry.BaseRegistryClient):
    """
    @brief Client class for accessing the ExchangeRegistry.
    @todo Stubbed only.  Needs much more work.
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "exchange_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)




factory = ProtocolFactory(ExchangeRegistryService)
