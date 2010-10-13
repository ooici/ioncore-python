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


class ExchangeRegistry(registry.BaseRegistryService):
    """
    @brief Skeleton exchange registry
    @todo flesh out
    """

    # Declaration of service
    declare = BaseService.service_declare(name='exchange_registry', version='0.1.0', dependencies=[])


    op_clear_registry = registry.BaseRegistryService.base_clear_registry
    """
    Service operation: clear registry
    """

    
    op_register = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update a pubsub.
    """

    
    op_get = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get pubsub
    """

    
    op_find = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find pubsub by characteristics
    """



# Spawn of the process using the module name
factory = ProtocolFactory(ExchangeRegistry)


class ExchangeRegistryClient(registry.BaseRegistryClient):
    """
    @brief Client class for accessing the ExchangeRegistry.
    @todo Stubbed only.  Needs much more work.
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "exchange_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)


    def clear_registry(self):
        return self.base_clear_registry('clear_registry')


    def register(self, name, exchange):
        """
        @brief Client method to register an exchange
        @param name is a string representing the name of this exchange
        @param exchange is a carrot connection
        """
        return  self.base_register_resource('register', name, exchange)


    def get(self, name):
        """
        @brief Get an exchange by name
        @param name is the string representing the name of the desired wxchange
        """
        return self.base_get_resource('get', name)


    def find(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @brief find all registered exchanges which match the attributes of description
        @see the registry docs for params
        """
        return self.base_find_resource('find',description,regex,ignore_defaults,attnames)
