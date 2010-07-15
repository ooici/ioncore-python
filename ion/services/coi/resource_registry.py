#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry.py
@author Michael Meisinger
@author David Stuebe
@brief service for registering resources
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

from ion.data import dataobject
from ion.data.datastore import registry

import re
from ion.data import store

from ion.core import ioninit
from ion.core import base_process
from ion.core.base_process import ProtocolFactory, BaseProcess
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

from ion.resources import coi_resource_descriptions

CONF = ioninit.config(__name__)


class ResourceRegistryService(registry.BaseRegistryService):
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
    declare = BaseService.service_declare(name='resource_registry', version='0.1.0', dependencies=[])

    
    op_clear_registry = registry.BaseRegistryService.base_clear_registry
    
    op_register_resource_instance = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register a resource instance with the registry.
    """
    op_register_resource_type = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update a resource type with the registry.
    """
    op_get_resource_instance = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a resource instance.
    """
    op_get_resource_type = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a resource type.
    """
    op_set_resource_lcstate = registry.BaseRegistryService.base_set_resource_lcstate
    """
    Service operation: Set a resource life cycle state
    """
    op_find_resource = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Set a resource life cycle state
    """

class ResourceRegistryClient(registry.BaseRegistryClient, registry.LCStateMixin):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "resource_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_registry')

    def register_local_resource_types(self):
        """
        """


    def register_resource_instance(self,resource):
        """
        Client method to Register a Resource instance
        """
        return self.base_register_resource(resource, 'register_resource_instance')
    
    def register_resource_type(self,resource):
        """
        Client method to register the Definition of a Resource Type
        """
        resource_type = self._describe_resource(resource)
        
        return self.base_register_resource(resource_type, 'register_resource_type')

    def _describe_resource(self,resource):
        resource_type = coi_resource_descriptions.ResourceDescription.create_new_resource()
        resource_type.name = resource.__class__.__name__
        resource_type.type = coi_resource_descriptions.OOIResourceTypes.information
        #resource_type.inherits_from = resource.__class__.
        if hasattr(resource, 'resource_description'):
            resource_type.description = resource.resource_description
            
        return resource_type
        

    def get_resource_type(self,resource_reference):
        """
        Get a resource type
        """
        return self.base_get_resource(resource_reference,'get_resource_type')

    def get_resource(self,resource_reference):
        """
        Get a resource instance
        """
        return self.base_get_resource(resource_reference,'get_resource_instance')
        
    def set_resource_lcstate(self, resource_reference, lcstate):
        return self.base_set_resource_lcstate(resource_reference, lcstate, 'set_resource_lcstate')

    def find_resource(self, description,regex=True,ignore_defaults=True):
        return self.base_find_resource(description,'find_resource',regex,ignore_defaults)


# Spawn of the process using the module name
factory = ProtocolFactory(ResourceRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""
