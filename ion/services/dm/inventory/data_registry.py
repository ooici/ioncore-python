#!/usr/bin/env python

"""
@file ion/services/dm/inventory/data_registry.py
@author Michael Meisinger
@author David Stuebe
@brief service for registering datas
"""

import logging
log = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect
from magnet.spawnable import Receiver

from ion.data import dataobject
from ion.data.datastore import registry
from ion.data import store

from ion.core import ioninit
from ion.core import base_process
from ion.core.base_process import ProtocolFactory, BaseProcess
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

from ion.resources import dm_resource_descriptions

CONF = ioninit.config(__name__)

class DataRegistryService(registry.BaseRegistryService):
    """
    @Brief Dataset registry service interface
    """
 
     # Declaration of service
    declare = BaseService.service_declare(name='data_registry', version='0.1.0', dependencies=[])

    op_define_data = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update a data resource.
    """
    op_get_data = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get data description
    """
    op_find_data = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find data resource by characteristics
    """


        
# Spawn of the process using the module name
factory = ProtocolFactory(DataRegistryService)


class DataRegistryClient(registry.BaseRegistryClient):
    """
    Class for the client accessing the data registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_registry')


    def define_data(self,data):
        """
        @Brief Client method to Register a Dataset
        
        @param data is an instance of a data resource
        """
        return  self.base_register_resource('define_data', data)    

    
    def get_data(self,data_reference):
        """
        @Brief Get a data by reference
        @param data_reference is the unique reference object for a registered
        data
        """
        return self.base_get_resource('get_data',data_reference)
        
    def find_data(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered datas which match the attributes of description
        @param see the registry docs for params
        """
        return self.base_find_resource('find_data',description,regex,ignore_defaults,attnames)




