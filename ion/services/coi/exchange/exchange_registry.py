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
    op_get_exchangename_by_id = registry.BaseRegistryService.base_get_resource_by_id
    op_find_exchangename = registry.BaseRegistryService.base_find_resource
    
    # name spaces
    op_register_exchangenamespace = registry.BaseRegistryService.base_register_resource
    op_get_exchangenamespace_by_id = registry.BaseRegistryService.base_get_resource_by_id
    op_find_exchangenamespace = registry.BaseRegistryService.base_find_resource

    # amqp mapping
    op_register_amqpmapping = registry.BaseRegistryService.base_register_resource
    op_get_exchangename_by_id = registry.BaseRegistryService.base_get_resource_by_id
    op_find_amqpmapping = registry.BaseRegistryService.base_find_resource

    # hardware mapping
    op_register_hardwaremapping = registry.BaseRegistryService.base_register_resource
    op_get_hardwaremapping_by_id = registry.BaseRegistryService.base_get_resource_by_id
    op_find_hardwaremapping = registry.BaseRegistryService.base_find_resource

    # broker federations
    op_register_brokerfederation = registry.BaseRegistryService.base_register_resource
    op_get_brokerfederation_by_id = registry.BaseRegistryService.base_get_resource_by_id
    op_find_brokerfederation = registry.BaseRegistryService.base_find_resource

    # broker credentials 
    op_register_brokercredentials = registry.BaseRegistryService.base_register_resource
    op_get_brokercredentials_by_id = registry.BaseRegistryService.base_get_resource_by_id
    op_find_brokercredentials = registry.BaseRegistryService.base_find_resource

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


    # EXCHANGE NAME
    def register_exchangename(self, exchangename):
        return self.base_register_resource('register_exchangename', exchangename)

    def get_exchangename_by_id(self, id):
        return self.base_get_resource_by_id('get_exchangename_by_id', id)

    def find_exchangename(self, description, regex=True,ignore_defaults=True,attnames=[]):
        return self.base_find_resource('find_exchangename', description, regex, ignore_defaults)


    # AMQP MAPPING
    def register_amqpmapping(self, amqpmapping):
        return self.base_register_resource('register_amqpmapping', amqpmapping)

    def get_ampqmapping_by_id(self, id):
        return self.base_get_resource_by_id('get_amqpmapping_by_id', id)

    def find_amqpmapping(self, description, regex=True,ignore_defaults=True,attnames=[]):
        return self.base_find_resource('find_amqpmapping', description, regex, ignore_defaults)


    # HARDWARE MAPPING
    def register_hardwaremapping(self, hardwaremapping):
        return self.base_register_resource('register_hardwaremapping', hardwaremapping)

    def get_hardwaremapping_by_id(self, id):
        return self.base_get_resource_by_id('get_hardwaremapping_by_id', id)

    def find_hardwaremapping(self, description, regex=True,ignore_defaults=True,attnames=[]):
        return self.base_find_resource('find_hardwaremapping', description, regex, ignore_defaults)


    # BROKER FEDERATION
    def register_brokerfederation(self, brokerfederation):
        return self.base_register_resource('register_brokerfederation', brokerfederation)

    def get_brokerfederation_by_id(self, id):
        return self.base_get_resource_by_id('get_brokerfederation_by_id', id)

    def find_brokerfederation(self, description, regex=True,ignore_defaults=True,attnames=[]):
        return self.base_find_resource('find_brokerfederation', description, regex, ignore_defaults)


    #BROKERCREDENTIALS
    def register_brokercredentials(self, brokercredentials):
        return self.base_register_resource('register_brokerfederation', brokercredentials)

    def get_brokercredentials_by_id(self, id):
        return self.base_get_resource_by_id('get_brokercredentials_by_id', id)

    def find_brokercredentials(self, description, regex=True,ignore_defaults=True,attnames=[]):
        return self.base_find_resource('find_brokercredentials', description, regex, ignore_defaults)


factory = ProcessFactory(ExchangeRegistryService)
