#!/usr/bin/env python


"""
@file ion/services/dm/pubsub/pubsub_registry.py
@author David Stuebe
@brief registry service for data publication and subscription
"""

from twisted.internet import defer

from ion.core import bootstrap
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient



from ion.data import dataobject
from ion.data.datastore import registry
from ion.data import store

class DataPubsubRegistryService(registry.BaseRegistryService):
    """
    A very simple registry for Data Pub Sub
    @TODO make the interface more specific for different kinds of pubsub objects
    Need to specify topic, publisher, subscriber
    """
 
     # Declaration of service
    declare = BaseService.service_declare(name='datapubsub_registry', version='0.1.0', dependencies=[])

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
factory = ProtocolFactory(DataPubsubRegistryService)


class DataPubsubRegistryClient(registry.BaseRegistryClient):
    """
    Class for the client accessing the Data PubSub Registry.
    @Todo clean up the interface for specific pubsub resource objects
    Need to specify topic, publisher, subscriber
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "datapubsub_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_registry')


    def register(self,pubsub):
        """
        @Brief Client method to Register Pubsub resources
        @param dataset is an instance of a dataset resource
        """
        return  self.base_register_resource('register', pubsub)    

    
    def get(self,pubsub_reference):
        """
        @Brief Get a pubsub resource by reference
        @param dpubsub_resource_reference is the unique reference object for a registered resource
        """
        return self.base_get_resource('get',pubsub_reference)
        
    def find(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered datasets which match the attributes of description
        @param see the registry docs for params
        """
        return self.base_find_resource('find',description,regex,ignore_defaults,attnames)

