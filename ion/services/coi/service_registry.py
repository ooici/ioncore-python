#!/usr/bin/env python

"""
@file ion/services/coi/service_registry.py
@author Michael Meisinger
@brief service for registering service (types and instances).
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

from ion.core import base_process
import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient


from ion.data.datastore import registry
from ion.data import dataobject
from ion.data import store

from ion.resources import coi_resource_descriptions


from ion.core import ioninit
CONF = ioninit.config(__name__)


class ServiceRegistryService(registry.BaseRegistryService):
    """
    Service registry service interface
    @todo a service is a resource and should also be living in the resource registry
    """
    # Declaration of service
    declare = BaseService.service_declare(name='service_registry', version='0.1.0', dependencies=[])

    op_clear_registry = registry.BaseRegistryService.base_clear_registry

    op_register_service_defintion = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register a service description with the registry.
    """

    op_get_service_definition = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a service description.
    """

    op_register_service_instance = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Register a service instance with the registry.
    """

    op_get_service_instance = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get a service instance.
    """
    op_set_service_lcstate = registry.BaseRegistryService.base_set_resource_lcstate
    """
    Service operation: Set a service life cycle state
    """
    op_find_service_definition = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find the definition of a service
    """
    op_find_described_resource = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find service definitions which meet a description
    """
    

# Spawn of the process using the module name
factory = ProtocolFactory(ServiceRegistryService)


class ServiceRegistryClient(registry.BaseRegistryClient, registry.LCStateMixin):
    """
    Client class for accessing the service registry. This is most important for
    finding and accessing any other services. This client knows how to find the
    service registry - what does that mean, don't all clients have targetname
    assigned?
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "service_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    def clear_registry(self):
        return self.base_clear_registry('clear_registry')

    def register_container_services(self):
        """
        This method is called when the container is started to inspect the
        services of lca arch and register descriptions for each service.
        """

    @defer.inlineCallbacks
    def register_service_defintion(self,service_class):
        """
        Client method to register the Definition of a Service Class
        """
        found = yield self.find_service_definition(service_class)
        if found:
            defer.returnValue(found)
            
        service_description = coi_resource_descriptions.ServiceDescription.create_new_resource()
        service_description.describe_service(service_class)
                        
        
        service_description.set_lifecyclestate(dataobject.LCStates.developed)
        service_description = yield self.base_register_resource(service_description, 'register_service_definition')
        defer.returnValue(resource_type)
        

    def get_service_description(self, resource_reference):
        """
        Get a service description
        """
        return self.base_get_resource(resource_reference,'get_service_description')

    @defer.inlineCallbacks
    def register_service_instance(self,service_instance):
        """
        Client method to register a Service Instance
        """
        found = yield self.find_service_instnace(service_instance)
        if found:
            defer.returnValue(found)
            
        service_reference = coi_resource_descriptions.ServiceInstance.create_new_resource()
        service_reference.describe_instance(service_class)
                        
        
        service_description.set_lifecyclestate(dataobject.LCStates.developed)
        service_description = yield self.base_register_resource(service_description, 'register_service_definition')
        defer.returnValue(resource_type)

    def get_service_instance(self, resource_reference):
        """
        Get a service instance
        """
        return self.base_get_resource(resource_reference,'get_service_instance')


    def set_resource_lcstate(self, resource_reference, lcstate):
        return self.base_set_resource_lcstate(resource_reference, lcstate, 'set_service_lcstate')


    @defer.inlineCallbacks
    def find_service_definition(self, resource):
        svc_desc = coi_resource_descriptions.ResourceDescription()
        svc_desc.describe_service(resource)
        alist = yield self.base_find_resource(resource_type,'find_resource_definition',regex=False,ignore_defaults=True)
        # Find returns a list but only one resource should match!
        if alist:
            assert len(alist) == 1
            defer.returnValue(alist[0])
        else:
            defer.returnValue(None)
            
    def find_described_resources(self, description,regex=True,ignore_defaults=True):
        return self.base_find_resource(description,'find_described_resource',regex,ignore_defaults)
        












