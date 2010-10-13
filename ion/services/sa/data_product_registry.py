#!/usr/bin/env python

"""
@file ion/services/sa/data_product_registry.py
@author Michael Meisinger
@brief service for registering data products
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from ion.core.cc.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.data.datastore import registry
from ion.resources import sa_resource_descriptions

class DataProductRegistryService(registry.BaseRegistryService):
    """
    Service that provides a registry for data products etc.
    Data products are for instance
    created as a pipeline of refining raw data through automatic and manual
    QAQC processes.
    Based on the BaseRegistryService.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_product_registry',
                                          version='0.1.0',
                                          dependencies=[])


    op_clear_data_product_registry = registry.BaseRegistryService.base_clear_registry
    """
    Service operation: Clears all records from the data product registry.
    """

    op_register_data_product = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register a data product with the registry.
    """

    op_get_data_product = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a data product.
    """

    op_find_data_product = registry.BaseRegistryService.base_find_resource
    """
    Service operation: find an data product
    """

class DataProductRegistryClient(registry.BaseRegistryClient, registry.LCStateMixin):
    """
    Class for the client accessing the data product registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_product_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    def clear_registry(self):
        return self.base_clear_registry('clear_data_product_registry')

    def register_data_product(self,data_product_instance):
        """
        Client method to Register a Resource instance
        """
        return self.base_register_resource('register_data_product', data_product_instance)

    def get_data_product(self,data_product_instance_reference):
        """
        Get a resource instance
        """
        return self.base_get_resource('get_data_product',resource_reference)

    def find_data_product(self, description, regex=True, ignore_defaults=True):
        return self.base_find_resource('find_data_product',description, regex, ignore_defaults)


# Spawn of the process using the module name
factory = ProtocolFactory(DataProductRegistryService)
