#!/usr/bin/env python


"""
@file ion/services/coi/exchange/exchange_registry.py
@author Brian Fox
@brief provides a registry service for exchange spaces and exchange points
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.data.datastore import registry

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


class ExchangeRegistryService(registry.BaseRegistryService):
    """
    @brief Skeleton exchange registry
    @todo flesh out
    """

    # A bunch of crud like interfaces for ...   
    
    # AMQP exchanges
    op_register_exchangename = registry.BaseRegistryService.base_register_resource
    op_x_exchangename = registry.BaseRegistryService.base_get_resource
    op_y_exchangename = registry.BaseRegistryService.base_find_resource
    
    # name spaces
    op_register_exchangenamespace = registry.BaseRegistryService.base_register_resource
    op_x_exchangenamespace = registry.BaseRegistryService.base_get_resource
    op_y_exchangenamespace = registry.BaseRegistryService.base_find_resource

    # amqp mapping
    op_register_amqpmapping = registry.BaseRegistryService.base_register_resource
    op_x_exchangenamespace = registry.BaseRegistryService.base_get_resource
    op_y_exchangenamespace = registry.BaseRegistryService.base_find_resource

    # hardware mapping
    op_register_hardwaremapping = registry.BaseRegistryService.base_register_resource
    op_x_exchangenamespace = registry.BaseRegistryService.base_get_resource
    op_y_exchangenamespace = registry.BaseRegistryService.base_find_resource

    # broker federations
    op_register_brokerfederation = registry.BaseRegistryService.base_register_resource
    op_x_brokerfederation = registry.BaseRegistryService.base_get_resource
    op_y_brokerfederation = registry.BaseRegistryService.base_find_resource

    # broker credentials 
    op_register_brokercredentials = registry.BaseRegistryService.base_register_resource
    op_x_brokercredentials = registry.BaseRegistryService.base_get_resource
    op_y_brokercredentials = registry.BaseRegistryService.base_find_resource

    declare = ServiceProcess.service_declare(name='exchange_registry', version='0.1.0', dependencies=[])




class ExchangeRegistryClient(registry.BaseRegistryClient):
    """
    @brief Client class for accessing the ExchangeRegistry.
    @todo Stubbed only.  Needs much more work.
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "exchange_registry"
        ServiceClient.__init__(self, proc, **kwargs)

    def register_exchangename(self, exchangename):
        """
        Client method to Register a Resource instance
        """
        return self.base_register_resource('register_exchangename', exchangename)


    def register_amqpmapping(self, amqpmapping):
        """
        Client method to Register a Resource instance
        """
        return self.base_register_resource('register_amqpmapping', amqpmapping)


    def register_hardwaremapping(self, hardwaremapping):
        """
        Client method to Register a Resource instance
        """
        return self.base_register_resource('register_hardwaremapping', hardwaremapping)


factory = ProcessFactory(ExchangeRegistryService)
