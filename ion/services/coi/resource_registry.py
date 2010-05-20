#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry.py
@author Michael Meisinger
@brief service for registering resources
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver
from ion.data.store import Store

from ion.core import base_process
from ion.core.base_process import ProtocolFactory, BaseProcess
from ion.data.dataobject import DataObject
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

class ResourceRegistryService(BaseService):
    """
    Resource registry service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='resource_registry', version='0.1.0', dependencies=[])

    # For now, keep registration in local memory store.
    def slc_init(self):
        self.datastore = Store()
    
    @defer.inlineCallbacks
    def op_register_resource(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        """
        resdesc = content['res_desc'].copy()
        logging.info('op_register_resource: '+str(resdesc))
        resdesc['lifecycle_state'] = ResourceLCState.RESLCS_NEW
        resid = pu.create_unique_id('R:')
        yield self.datastore.put(resid, resdesc)
        yield self.reply(msg, 'result', {'res_id':str(resid)},)        

    def op_define_resource_type(self, content, headers, msg):
        """
        Service operation: Create or update a resource type with the registry.
        """

    @defer.inlineCallbacks
    def op_get_resource_desc(self, content, headers, msg):
        """
        Service operation: Get description for a resource instance.
        """
        resid = content['res_id']
        logging.info('op_get_resource_desc: '+str(resid))

        res_desc = yield self.datastore.get(resid)
        yield self.reply(msg, 'result', {'res_desc':res_desc})        

    def op_set_resource_lcstate(self, content, headers, msg):
        """
        Service operation: set the life cycle state of resource
        """
        
    def op_find_resources(self, content, headers, msg):
        """
        Service operation: find resources by criteria
        """

        
class ResourceRegistryClient(BaseServiceClient):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, pid=None):
        BaseServiceClient.__init__(self, "resource_registry", proc, pid)

    def registerResourceType(self, rt_desc):
        pass

    @defer.inlineCallbacks
    def register_resource(self, res_desc):
        yield self._check_init()

        (content, headers, msg) = yield self.proc.rpc_send(self.svc,
                        'register_resource', {'res_desc':res_desc.encode()})
        logging.info('Service reply: '+str(headers))
        defer.returnValue(str(content['res_id']))

    @defer.inlineCallbacks
    def get_resource_desc(self, res_id):
        yield self._check_init()

        (content, headers, msg) = yield self.proc.rpc_send(self.svc,
                        'get_resource_desc', {'res_id':res_id})
        logging.info('Service reply: '+str(content))
        rd = ResourceDesc()
        rdd = content['res_desc']
        if rdd != None:
            rd.decode(rdd)
            defer.returnValue(rd)
        else:
            defer.returnValue(None)
        
class ResourceTypes(object):
    """Static class with constant definitions for resource types.
    Do not instantiate
    """
    RESTYPE_GENERIC = 'rt_generic'
    RESTYPE_SERVICE = 'rt_service'
    RESTYPE_UNASSIGNED = 'rt_unassigned'
    
    def __init__(self):
        raise RuntimeError('Do not instantiate '+self.__class__.__name__)

class ResourceLCState(object):
    """Static class with constant definitions for resource life cycle states.
    Do not instantiate
    """
    RESLCS_NEW = 'rlcs_new'
    RESLCS_ACTIVE = 'rlcs_active'
    RESLCS_INACTIVE = 'rlcs_inactive'
    RESLCS_DECOMM = 'rlcs_decomm'
    RESLCS_RETIRED = 'rlcs_retired'
    RESLCS_DEVELOPED = 'rlcs_developed'
    RESLCS_COMMISSIONED = 'rlcs_commissioned'
    
    def __init__(self):
        raise RuntimeError('Do not instantiate '+self.__class__.__name__)

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
            self.set_attr('res_type',kwargs['res_type'])
        else:
            raise RuntimeError("Resource type missing")
            
        if 'name' in kwargs:
            self.set_attr('res_name',kwargs['name'])

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
            self.set_attr('name',kwargs['name'])
        else:
            raise RuntimeError("Resource type name missing")

        if 'based_on' in kwargs:
            self.set_attr('based_on',kwargs['based_on'])
        else:
            self.based_on = ResourceTypes.RESTYPE_GENERIC

        if 'res_type' in kwargs:
            self.set_attr('res_type',kwargs['res_type'])
        else:
            self.res_type = ResourceTypes.RESTYPE_UNASSIGNED
    
        if 'desc' in kwargs:
            self.set_attr('desc',kwargs['desc'])

# Spawn of the process using the module name
factory = ProtocolFactory(ResourceRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""
