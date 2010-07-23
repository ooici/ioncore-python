#!/usr/bin/env python

"""
@file ion/services/dm/dataset_registry.py
@author Michael Meisinger
@brief service for registering datasets
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect
from magnet.spawnable import Receiver

from ion.data import dataobject
from ion.data.datastore import registry
from ion.data import store

import inspect

from ion.core import ioninit
from ion.core import base_process
from ion.core.base_process import ProtocolFactory, BaseProcess
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

from ion.resources import coi_resource_descriptions

CONF = ioninit.config(__name__)

class DatasetRegistryService(registry.BaseRegistryService):
    """Dataset registry service interface
    """
 
     # Declaration of service
    declare = BaseService.service_declare(name='dataset_registry', version='0.1.0', dependencies=[])

    op_define_dataset = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update a dataset resource.
    """
    op_get_dataset = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get dataset description
    """
    op_find_dataset = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find dataset resource by characteristics
    """


        
# Spawn of the process using the module name
factory = ProtocolFactory(DatasetRegistryService)


class DatasetRegistryClient(registry.BaseRegistryClient):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "dataset_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_registry')


    def define_dataset(self,dataset):
        """
        @Brief Client method to Register a Dataset
        
        @param dataset is an instance of a dataset resource
        """
        return  self.base_register_resource('define_dataset', dataset)    

    
    def get_dataset(self,dataset_reference):
        """
        @Brief Get a dataset by reference
        @param dataset_reference is the unique reference object for a registered
        dataset
        """
        return self.base_get_resource(dataset_reference,'get_dataset')
        
    def find_dataset(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered datasets which match the attributes of description
        @param see the registry docs for params
        """
        return self.base_find_resource('find_dataset',description,regex,ignore_defaults,attnames)




