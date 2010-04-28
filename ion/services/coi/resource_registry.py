#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry.py
@author Michael Meisinger
@brief service for registering resources
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver
from magnet.store import Store

from ion.core.base_process import RpcClient
from ion.data.dataobject import DataObject
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

class ResourceRegistryService(BaseService):
    """Resource registry service interface
    """
    datastore = Store()
    
    @defer.inlineCallbacks
    def op_register_resource(self, content, headers, msg):
        resdesc = content['res_desc'].copy()
        logging.info('op_register_resource: '+str(resdesc))
        resdesc.lifecycle_state = ResourceLCState.RESLCS_NEW
        resid = pu.create_unique_id('R:')
        
        yield self.datastore.put(resid,resdesc)
        yield self.reply_message(msg, 'result', {'res_id':str(resid)}, {})        
        
class ResourceRegistryClient(BaseServiceClient):
    """Class for
    """
    
    def registerResourceType(self, rt_desc):
        pass

    @defer.inlineCallbacks
    def registerResource(self, res_desc):
        self.rpc = RpcClient()
        yield self.rpc.attach()

        resid = yield self.rpc.rpc_send(to, 'register_resource', {'res_desc':res_desc}, {})
        logging.info('Service reply: '+str(resid))

        
        
class ResourceTypes(object):
    """Static class with constant definitions. Do not instantiate
    """
    RESTYPE_GENERIC = 'rt_generic'
    RESTYPE_SERVICE = 'rt_service'
    RESTYPE_UNASSIGNED = 'rt_unassigned'
    
    def __init__(self):
        raise RuntimeException('Do not instantiate '+self.__class__.__name__)

class ResourceLCState(object):
    """Static class with constant definitions. Do not instantiate
    """
    RESLCS_NEW = 'rlcs_new'
    RESLCS_ACTIVE = 'rlcs_active'
    RESLCS_INACTIVE = 'rlcs_inactive'
    RESLCS_DECOMM = 'rlcs_decomm'
    RESLCS_RETIRED = 'rlcs_retired'
    
    def __init__(self):
        raise RuntimeException('Do not instantiate '+self.__class__.__name__)

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

"""
from ion.services.coi.resource_registry import *
rd2 = ResourceDesc(name='res2',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()

"""
