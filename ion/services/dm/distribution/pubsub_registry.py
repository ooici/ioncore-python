#!/usr/bin/env python


"""
@file ion/services/dm/distribution/pubsub_registry.py
@author David Stuebe
@brief registry service for data topics, publication & subscription
"""

#from ion.core import bootstrap
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.data.datastore import registry
#from ion.services.dm.preservation import store

class DataPubsubRegistryService(registry.BaseRegistryService):
    """
    @brief A very simple registry for Data Pub Sub
    @todo make the interface more specific for different kinds of pubsub objects
    Need to specify topic, publisher, subscriber
    """

     # Declaration of service
    declare = ServiceProcess.service_declare(name='datapubsub_registry', version='0.1.0', dependencies=[])

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
factory = ProcessFactory(DataPubsubRegistryService)


class DataPubsubRegistryClient(registry.BaseRegistryClient):
    """
    @brief Class for the client accessing the Data PubSub Registry.
    @todo clean up the interface for specific pubsub resource objects
    Need to specify topic, publisher, subscriber
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "datapubsub_registry"
        ServiceClient.__init__(self, proc, **kwargs)


    def clear_registry(self):
        return self.base_clear_registry('clear_registry')


    def register(self,pubsub):
        """
        @brief Client method to Register Pubsub resources
        @param dataset is an instance of a dataset resource
        """
        return  self.base_register_resource('register', pubsub)


    def get(self,pubsub_reference):
        """
        @brief Get a pubsub resource by reference
        @param dpubsub_resource_reference is the unique reference object for a registered resource
        """
        return self.base_get_resource('get',pubsub_reference)

    def find(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @brief find all registered datasets which match the attributes of description
        @see the registry docs for params
        """
        return self.base_find_resource('find',description,regex,ignore_defaults,attnames)
