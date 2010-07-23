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

import inspect

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
    op_get_resource_instance = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a resource instance.
    """
    
    op_register_resource_definition = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update a resource definition with the registry.
    """
    op_get_resource_definition = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a resource definition.
    """
    
    op_set_resource_lcstate = registry.BaseRegistryService.base_set_resource_lcstate
    """
    Service operation: Set a resource life cycle state
    """
    
    op_find_registered_resource_definition_from_resource = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the registered definition of a resource
    """
    op_find_registered_resource_definitions_from_description = registry.BaseRegistryService.base_find_resource
    """
    Service operation: find all registered resources which match the attributes of description
    """
    
    op_find_registered_resource_instance_from_instance = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the registered instances that matches the service class
    """
    op_find_registered_resource_instance_from_description = registry.BaseRegistryService.base_find_resource
    """
    Service operation: find all registered resources which match the attributes of description
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
    def register_resource_instance(self,resource,owner=None):
        """
        Client method to Register a Resource instance
        An instance is a reference to an owner, a description and the resource
        @Note this need architectural clarification
        """
        # Can't have an resource instance for a resource instance - Is that a problem? 
        if isinstance(resource, coi_resource_descriptions.ResourceInstance):
            resource_instance_description = resource
            assert resource_instance_description.RegistryIdentity, 'Resource Instance Description must have a registry Identity'
        else:
            resource_instance = resource
            # Build a new description of the resource instance
            resource_instance_description = yield self.describe_instance(resource_instance,owner)
            
            found_rid = yield self.find_registered_resource_instance_from_description(resource_instance,regex=False,ignore_defaults=True)
            if found_rid:
                assert len(found_rid) == 1
                defer.returnValue(found_rid[0])
            else:
                # Give it a new Unique ID and put it in the registry
                resource_instance_description.create_new_reference()
                resource_instance_description.set_lifecyclestate(dataobject.LCStates.developed)
                
        resource_instance_description = yield self.base_register_resource('register_resource_instance', resource_instance_description)    
        defer.returnValue(resource_instance_description)
        
    @defer.inlineCallbacks
    def describe_instance(self,resource_instance,owner):
        """
        @Brief Extract metadata from a resource instance to store in the resource
        registry
        @Param resource is an instance of a class which inherits from Resource
        """
        assert isinstance(resource_instance, dataobject.Resource)
        assert isinstance(owner, coi_resource_descriptions.IdentityResource)

        # Do not make a new resource idenity - this is a generic method which
        # is also used to look for an existing description
        resource_instance_description = coi_resource_descriptions.ResourceInstance()
        resource_instance_description.name = resource_instance.__class__.__name__ 

        #Get the registry description for this resource
        resource_description = yield self.register_resource_definition(resource_instance.__class__)
        resource_instance_description.description = resource_description.reference(head=True)

        resource_instance_description.owner = owner.reference(head=True)
        resource_instance_description.resource = resource_instance.reference(head=True)
        
        # Set the life cycle state to developed?
        resource_instance_description.set_lifecyclestate(dataobject.LCStates.developed)
        
        defer.returnValue(resource_instance_description)
        
    def get_resource_instance(self,resource_reference):
        """
        Get a resource instance
        """
        return self.base_get_resource(resource_reference,'get_resource_instance')
        
        
    @defer.inlineCallbacks
    def register_resource_definition(self,resource):
        """
        Client method to register the Definition of a Resource Type
        """
        if isinstance(resource, coi_resource_descriptions.ResourceDescription):
            resource_description = resource
            assert resource_description.RegistryIdentity, 'Resource Description must have a registry Identity'

        else:
            resource_class = resource
            # Build a new description of the resource
            resource_description = yield self.describe_resource(resource_class)

            found_rd = yield self.find_registered_resource_definitions_from_description(resource_description,regex=False,ignore_defaults=True)
            if found_rd:
                assert len(found_rd) == 1
                defer.returnValue(found_rd[0])
            else:
                resource_description.create_new_reference()
            
        resource_description = yield self.base_register_resource('register_resource_definition', resource_description)
        defer.returnValue(resource_description)
        
    @defer.inlineCallbacks
    def describe_resource(self,resource_class):
        """
        @Brief Extract metadata from a resource subclass to store in the resource
        registry
        @Param resource is a class which inherits from Resource
        """
        assert issubclass(resource_class, dataobject.Resource)
        
        # Do not make a new resource idenity - this is a generic method which
        # is also used to look for an existing description
        resource_description = coi_resource_descriptions.ResourceDescription()        
        resource_description.name = resource_class.__name__
        resource_description.description = inspect.getdoc(resource_class)
                
        # Get all the typed attributes of the resource
        for name, att in resource_class.get_typedattributes().items():
            attdesc = coi_resource_descriptions.AttributeDescription()
            attdesc.name = name
            attdesc.type = str(att.type)
            attdesc.default = str(att.default)
            resource_description.atts.append(attdesc)    
        
        
        # Get the reference to the resource it inherits from
        
        base_ref = yield self.get_resource_bases_by_reference(resource_class)
                
        resource_description.inherits_from = base_ref
        
        # Set the type based on inheritance?
        if issubclass(resource_class, dataobject.InformationResource):
            resource_description.type = coi_resource_descriptions.OOIResourceTypes.information
        elif issubclass(resource_class, dataobject.StatefulResource):
            resource_description.type = coi_resource_descriptions.OOIResourceTypes.stateful
        else:
            resource_description.type = coi_resource_descriptions.OOIResourceTypes.unassigned
        
        # Update the lifecycle state and return
        #resource_description.set_lifecyclestate(dataobject.LCStates.developed)
        defer.returnValue(resource_description)
        
        
        

    @defer.inlineCallbacks
    def get_resource_bases_by_reference(self,resource_class):
        
        bases = self.get_parent_resource_classes(resource_class)
        
        bases_refs=[]
        for base in bases:
            parent_resource_description = yield self.find_registered_resource_definition_from_resource(base)
            if not parent_resource_description:
                parent_resource_description = yield self.register_resource_definition(base)
            
            bases_refs.append(parent_resource_description.reference())

        defer.returnValue(bases_refs)
        

    def get_parent_resource_classes(self,resource_class):
        assert issubclass(resource_class, dataobject.Resource)
        # Ignore multiple inheritence for now!
        bases=[]
        for base in resource_class.__bases__:
            if issubclass(base, dataobject.Resource):
                bases.append(base)
        return bases
        

    def get_resource_definition(self,resource_reference):
        """
        Get a resource definition
        """
        return self.base_get_resource('get_resource_definition',resource_reference)

        
    def set_resource_lcstate(self, resource_reference, lcstate):
        return self.base_set_resource_lcstate('set_resource_lcstate',resource_reference, lcstate)

    @defer.inlineCallbacks
    def find_registered_resource_definition_from_resource(self, resource_class):
        """
        @Brief find the registered definition of a resoruce
        """
        resource_description = yield self.describe_resource(resource_class)
        description_list = yield self.base_find_resource('find_registered_resource_definition_from_resource',resource_description,regex=False,ignore_defaults=True)
        # Find returns a list but only one resource should match!
        if description_list:
            assert len(description_list) == 1
            defer.returnValue(description_list[0])
        else:
            defer.returnValue(None)
            
    def find_registered_resource_definitions_from_description(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered resources which match the attributes of description
        """
        return self.base_find_resource('find_registered_resource_definitions_from_description',description,regex,ignore_defaults,attnames)
        
    @defer.inlineCallbacks
    def find_registered_resource_instance_from_instance(self, resource_instance, owner):
        """
        @Brief Find the registered instance of a resource
        """
        resource_instance_description = yield self.describe_instance(resource_instance,owner)
        resource_list = yield self.base_find_resource('find_registered_resource_instance_from_instance',resource_instance_description,regex=False,ignore_defaults=True)
        # Find returns a list but only one resource should match!
        if resource_list:
            assert len(resource_list) == 1
            defer.returnValue(resource_list[0])
        else:
            defer.returnValue(None)

    def find_registered_resource_instance_from_description(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered resources which match the attributes of description
        """
        return self.base_find_resource('find_registered_resource_instance_from_description',description,regex,ignore_defaults,attnames)

# Spawn of the process using the module name
factory = ProtocolFactory(ResourceRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""
