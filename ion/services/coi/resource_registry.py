#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry.py
@author Michael Meisinger
@brief service for registering resources
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

from ion.core.base_process import RpcClient
from ion.data.dataobject import DataObject
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

class ResourceRegistryService(BaseService):
    """Resource registry service interface
    """

class ResourceRegistryClient(BaseServiceClient):
    """Class for
    """
    
class ResourceTypes(object):
    """Static class with constant definitions. Do not instantiate
    """
    RESTYPE_GENERIC = 'rt_generic'
    RESTYPE_SERVICE = 'rt_service'
    RESTYPE_UNASSIGNED = 'rt_unassigned'
    
    def __init__(self):
        raise RuntimeException('Do not instantiate '+__name__)

class ResourceDesc(DataObject):
    """Structured object for a resource description.

    Attributes:
    .name   name of the resource type
    .res_type   identifier of the resource's type
    """
    def __init__(self, **kwargs):
        DataObject.__init__(self)
        if len(kwargs) != 0:
            self.setResourceDesc(**kwargs)

    def setResourceDesc(self, **kwargs):
        if 'res_type' in kwargs:
            self.res_type = kwargs['res_type']
        else:
            raise RuntimeException("Resource type missing")
            
        if 'name' in kwargs:
            self.res_name = kwargs['name']

class ResourceTypeDesc(DataObject):
    """Structured object for a resource type description.
    
    Attributes:
    .res_name   name of the resource type
    .res_type   identifier of this resource type
    .based_on   identifier of the base resource type
    .desc   description
    """
    def __init__(self, **kwargs):
        DataObject.__init__(self)
        if len(kwargs) != 0:
            self.setResourceTypeDesc(**kwargs)
        
    def setResourceTypeDesc(self, **kwargs):
        if 'name' in kwargs:
            self.name = kwargs['name']
        else:
            raise RuntimeException("Resource type name missing")

        if 'based_on' in kwargs:
            self.based_on = kwargs['based_on']
        else:
            self.based_on = ResourceTypes.RESTYPE_GENERIC

        if 'res_type' in kwargs:
            self.res_type = kwargs['res_type']
        else:
            self.res_type = ResourceTypes.RESTYPE_UNASSIGNED
    
        if 'desc' in kwargs:
            self.desc = kwargs['desc']

# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = ResourceRegistryService(receiver)


