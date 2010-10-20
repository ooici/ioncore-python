#!/usr/bin/env python

"""
@file ion/services/dm/inventory/data_registry.py
@author Michael Meisinger
@author David Stuebe
@brief service for registering datas
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect

from ion.data import dataobject
from ion.data.datastore import registry
from ion.services.dm.preservation import store

from ion.core import ioninit
from ion.core.process.process import ProcessFactory, Process
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.procutils as pu

from ion.resources import dm_resource_descriptions

CONF = ioninit.config(__name__)

class DataRegistryService(registry.BaseRegistryService):
    """
    @brief Dataset registry service interface
    """

     # Declaration of service
    declare = ServiceProcess.service_declare(name='data_registry', version='0.1.0', dependencies=[])

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
factory = ProcessFactory(DataRegistryService)


class DataRegistryClient(registry.BaseRegistryClient):
    """
    Class for the client accessing the data registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_registry"
        ServiceClient.__init__(self, proc, **kwargs)


    def clear_registry(self):
        return self.base_clear_registry('clear_registry')


    def define_data(self,data):
        """
        @brief Client method to Register a Dataset

        @param data is an instance of a data resource
        """
        return  self.base_register_resource('define_data', data)


    def get_data(self,data_reference):
        """
        @brief Get a data by reference
        @param data_reference is the unique reference object for a registered
        data
        """
        return self.base_get_resource('get_data',data_reference)

    def find_data(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @brief find all registered datas which match the attributes of description
        @param see the registry docs for params
        """
        return self.base_find_resource('find_data',description,regex,ignore_defaults,attnames)
