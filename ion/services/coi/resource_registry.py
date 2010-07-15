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

CONF = ioninit.config(__name__)


class ResourceRegistryService(BaseService):
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

    slc_init = registry.BaseRegistryService.slc_init

    op_clear_registry = registry.BaseRegistryService.op_clear_registry

    op_register_resource_instance = registry.BaseRegistryService.op_register_resource
    """
    Service operation: Register a resource instance with the registry.
    """
    
    op_define_resource_type = registry.BaseRegistryService.op_register_resource
    """
    Service operation: Create or update a resource type with the registry.
    """
    
    op_get_resource_instance = registry.BaseRegistryService.op_get_resource
    """
    Service operation: Get a resource instance.
    """
 
    op_get_resource_type = registry.BaseRegistryService.op_get_resource
    """
    Service operation: Get a resource type.
    """

    op_find_resource = registry.BaseRegistryService.op_find_resource


        

class ResourceRegistryClient(BaseServiceClient):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "resource_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    register_resource_instance = registry.BaseRegistryClient.register_resource
    """
    Client method to Register a Resource instance
    """
    
    define_resource_type = registry.BaseRegistryClient.register_resource
    """
    Client method to register the Definition of a Resource Type
    """
    
    get_resource_instance = registry.BaseRegistryClient.get_resource
    """
    Get a resource instance
    """
    
    get_resource_type = registry.BaseRegistryClient.get_resource
    """
    Get a resource type
    """

# Spawn of the process using the module name
factory = ProtocolFactory(ResourceRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""
