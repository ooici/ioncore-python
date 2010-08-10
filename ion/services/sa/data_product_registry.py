#!/usr/bin/env python

"""
@file ion/services/sa/data_product_registry.py
@author Michael Meisinger
@brief service for registering data products
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.data.datastore import registry


from ion.resources import sa_resource_descriptions

'''
class DataProductRegistryService(BaseService):
    """Data product registry service interface. Data products are for instance
    created as a pipeline of refining raw data through automatic and manual
    QAQC processes.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_product_registry', version='0.1.0', dependencies=[])
 
    def op_define_data_product(self, content, headers, msg):
        """Service operation: Create or update a data product definition.
        """

    def op_find_data_product(self, content, headers, msg):
        """Service operation: Find a data product definition by characteristics.
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(DataProductRegistryService)
'''

class DataProductRegistryService(registry.BaseRegistryService):
    """
    Resource registry service interface
    The Resource Registry Service uses an IStore interface to a backend Key
    Value Store to store to track version controlled objects. The store will
    share a name space and share objects depending on configuration when it is
    created. The resource are retrieved as complete objects from the store. The
    built-in encode method is used to store and transmit them using the COI
    messaging.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_product_registry', version='0.1.0', dependencies=[])

    
    op_clear_data_product = registry.BaseRegistryService.base_clear_registry #changed
    
    op_register_data_product_instance = registry.BaseRegistryService.base_register_resource #changed
    """
    Service operation: Register a resource instance with the registry.
    """
    op_register_data_product_type = registry.BaseRegistryService.base_register_resource #changed
    """
    Service operation: Create or update a resource type with the registry.
    """
    op_get_data_product_instance = registry.BaseRegistryService.base_get_resource #changed
    """
    Service operation: Get a resource instance.
    """
    op_get_data_product_type = registry.BaseRegistryService.base_get_resource #changed
    """
    Service operation: Get a resource type.
    """
    #op_set_data_product_lcstate = registry.BaseRegistryService.base_set_resource_lcstate #changed
    #"""
    #Service operation: Set a resource life cycle state
    #"""
    op_find_data_product_type = registry.BaseRegistryService.base_find_resource #changed
    """
    Service operation: find and data_product type
    """
    op_find_data_product_instance = registry.BaseRegistryService.base_find_resource #changed
    """
    Service operation: find an data_product instance
    """

class DataProductRegistryClient(registry.BaseRegistryClient, registry.LCStateMixin):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_product_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_data_product')

    def register_data_product_instance(self,data_product_instance):
        """
        Client method to Register a Resource instance
        """
        return self.base_register_resource('register_data_product_instance', data_product_instance)
    
    def register_data_product_type(self,data_product_type):
        """
        Client method to register the Definition of a Resource Type
        """
        return self.base_register_resource('register_data_product_type',data_product_type)

    def get_data_product_type(self,data_product_type_reference):
        """
        Get a data_product type
        """
        return self.base_get_resource('get_data_product_type',data_product_type_reference)

    def get_data_product_instance(self,data_product_instance_reference):
        """
        Get a resource instance
        """
        return self.base_get_resource('get_data_product_instance',resource_reference)
        
    #def set_resource_lcstate(self, resource_reference, lcstate):
    #    return self.base_set_resource_lcstate(resource_reference, lcstate, 'set_data_product_lcstate')

    def find_data_product_type(self, description,regex=True,ignore_defaults=True):
        return self.base_find_resource('find_data_product_type',description,regex,ignore_defaults)
        
    def find_data_product_instance(self, description,regex=True,ignore_defaults=True):
        return self.base_find_resource('find_data_product_instance',description,regex,ignore_defaults)


# Spawn of the process using the module name
factory = ProtocolFactory(DataProductRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""

        


