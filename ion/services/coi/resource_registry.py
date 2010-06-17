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
    """

    # Declaration of service
    declare = BaseService.service_declare(name='resource_registry', version='0.1.0', dependencies=[])

    # For now, keep registration in local memory store.
    @defer.inlineCallbacks
    def slc_init(self):
        # use spawn args to determine backend class, second config file
        backendcls = self.spawn_args.get('backend_class', CONF.getValue('backend_class', None))
        backendargs = self.spawn_args.get('backend_args', CONF.getValue('backend_args', {}))
        if backendcls:
            self.backend = pu.get_class(backendcls)
        else:
            self.backend = store.Store
        assert issubclass(self.backend, store.IStore)

        # Provide rest of the spawnArgs to init the store
        s = yield self.backend.create_store(**backendargs)
        
        self.reg = registry.ResourceRegistry(s)
        
        name = self.__class__.__name__
        logging.info(name + " initialized")
        logging.info(name + " backend:"+str(backendcls))
        logging.info(name + " backend args:"+str(backendargs))
        
        
        
    @defer.inlineCallbacks
    def op_register_resource(self, content, headers, msg):
        """
        Service operation: Register a resource instance with the registry.
        """
        res_id = str(content['res_id'])
        res_enc = content['res_enc']
        resource = registry.ResourceDescription.decode(res_enc)()
        logging.info('op_register_resource: \n' + str(resource))

        yield self.reg.register(res_id,resource)
        yield self.reply_ok(msg, {'res_id':str(res_id)},)

    def op_define_resource_type(self, content, headers, msg):
        """
        Service operation: Create or update a resource type with the registry.
        """

    @defer.inlineCallbacks
    def op_get_resource(self, content, headers, msg):
        """
        Service operation: Get a resource instance.
        """
        res_id = content['res_id']
        logging.info('op_get_resource: '+str(res_id))

        resource = yield self.reg.get_description(res_id)
        logging.info('Got Resource \n: '+str(resource))

        yield self.reply_ok(msg, {'res_enc':resource.encode()})

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
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "resource_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    def registerResourceType(self, rt_desc):
        pass

    @defer.inlineCallbacks
    def register_resource(self, res_id, resource):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('register_resource',
                                            {'res_id':res_id,'res_enc':resource.encode()})
        logging.info('Service reply: '+str(headers))
        defer.returnValue(str(content['res_id']))

    @defer.inlineCallbacks
    def get_resource(self, res_id):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_resource',
                                                      {'res_id':res_id})
        logging.info('Service reply: '+str(content))
        res_enc = content['res_enc']
        if res_enc != None:
            resource = registry.ResourceDescription.decode(res_enc)()
            defer.returnValue(resource)
        else:
            defer.returnValue(None)

#class ResourceTypes(object):
#    """Static class with constant definitions for resource types.
#    Do not instantiate
#    """
#    RESTYPE_GENERIC = 'rt_generic'
#    RESTYPE_SERVICE = 'rt_service'
#    RESTYPE_UNASSIGNED = 'rt_unassigned'
#
#    def __init__(self):
#        raise RuntimeError('Do not instantiate '+self.__class__.__name__)
#

# Spawn of the process using the module name
factory = ProtocolFactory(ResourceRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""
