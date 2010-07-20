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
    op_register_resource_definition = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update a resource definition with the registry.
    """
    op_get_resource_instance = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a resource instance.
    """
    op_get_resource_definition = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a resource definition.
    """
    op_set_resource_lcstate = registry.BaseRegistryService.base_set_resource_lcstate
    """
    Service operation: Set a resource life cycle state
    """
    op_find_resource_definition = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the definition of a resource
    """
    op_find_described_resource = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find resource definitions which meet the description
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

    def register_container_resources(self):
        """
        This method is called when the container is started to inspect the
        resources directory of lca arch and register descriptions for all
        resources.
        """

    @defer.inlineCallbacks
    def register_resource_instance(self,resource,owner):
        """
        Client method to Register a Resource instance
        An instance is a reference to an owner, a description and the resource
        @Note this need architectural clarification
        """
        resource_description = yield self.register_resource_definition(resource)

        resource_instance = coi_resource_descriptions.ResourceInstance.create_new_resource()
        resource_instance.name = resource.name # ?
        resource_instance.description = resource_description.reference(head=True)
        resource_instance.owner = owner.reference(head=True)
        resource_instance.resource = resource.reference(head=True)
            
        resource_instance = self.base_register_resource(resource_instance, 'register_resource_instance')
    
        defer.returnValue(resource_instance)
        
    @defer.inlineCallbacks
    def register_resource_definition(self,resource):
        """
        Client method to register the Definition of a Resource Type
        """
        
        found = yield self.find_resource_definition(resource)
        if found:
            defer.returnValue(found)
            
        resource_type = coi_resource_descriptions.ResourceDescription.create_new_resource()
        resource_type.describe_resource(resource)
                        
        if resource_type.name != 'Resource':
            # If it is the base Resource skip it!
            resource_type.inherits_from = yield self.get_parent_resource_reference(resource)
        
        resource_type.set_lifecyclestate(dataobject.LCStates.developed)
        resource_type = yield self.base_register_resource(resource_type, 'register_resource_definition')
        defer.returnValue(resource_type)
        

    @defer.inlineCallbacks
    def get_parent_resource_reference(self,resource):
        
        parent_class = self.get_resource_parent_class(resource)
        parent_resource_def = yield self.find_resource_definition(parent_class())
        if not parent_resource_def:
            parent_resource_def = yield self.register_resource_definition(parent_class())

        defer.returnValue(parent_resource_def.reference())
        

    def get_resource_parent_class(self,resource):
        assert isinstance(resource, dataobject.ResourceReference)
        # Ignore multiple inheritence for now!
        if issubclass(resource.__class__.__bases__[0], dataobject.Resource):
            return resource.__class__.__bases__[0]
        else:
            # 'Resource' should refer to itself!
            return dataobject.Resource
        

    def get_resource_definition(self,resource_reference):
        """
        Get a resource definition
        """
        return self.base_get_resource(resource_reference,'get_resource_definition')

    def get_resource_instance(self,resource_reference):
        """
        Get a resource instance
        """
        return self.base_get_resource(resource_reference,'get_resource_instance')
        
    def set_resource_lcstate(self, resource_reference, lcstate):
        return self.base_set_resource_lcstate(resource_reference, lcstate, 'set_resource_lcstate')

    @defer.inlineCallbacks
    def find_resource_definition(self, resource):
        """
        @Brief find the definition of a resoruce in the resource registry
        """
        resource_type = coi_resource_descriptions.ResourceDescription()
        resource_type.describe_resource(resource)
        alist = yield self.base_find_resource(resource_type,'find_resource_definition',regex=False,ignore_defaults=True)
        # Find returns a list but only one resource should match!
        if alist:
            assert len(alist) == 1
            defer.returnValue(alist[0])
        else:
            defer.returnValue(None)
            
    def find_described_resources(self, description,regex=True,ignore_defaults=True):
        """
        @Brief find all registered resources which match the attributes of description
        """
        return self.base_find_resource(description,'find_described_resource',regex,ignore_defaults)
        
    def find_resource_instance(self, description,regex=True,ignore_defaults=True):
        """
        @Brief Find all registered instances that match the the attributes of description
        """
        return self.base_find_resource(description,'find_resource_instance',regex,ignore_defaults)

# Spawn of the process using the module name
factory = ProtocolFactory(ResourceRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""
