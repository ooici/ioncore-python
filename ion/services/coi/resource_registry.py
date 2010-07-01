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

class BaseResourceRegistryService(BaseService):
    """
    Base class for resource registries
    @TODO make sure this is a pure virtual class
    """

    
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
    def op_set_resource_lcstate(self, content, headers, msg):
        """
        Service operation: set the life cycle state of resource
        """
        res_id = str(content['res_id'])
        lifecycle = str(content['lifecycle'])
        logging.info('op_set_resource_lcstate: '+str(res_id) + ', LCState:' + str(lifecycle))

        resource = yield self.reg.get_description(res_id)
        
        if resource:
            resource.set_lifecyclestate(registry.LCStates[lifecycle])
            yield self.reg.register(res_id,resource)
            yield self.reply_ok(msg, {'res_id':str(res_id)},)

        else:
            yield self.reply_err(msg, {'res_id':None})


    @defer.inlineCallbacks
    def op_find_resources(self, content, headers, msg):
        """
        Service operation: find resources by criteria
        Content is a dictionary of attributes which must match a resource:
        """

        reslist = yield self.reg.list_descriptions()
        find_list=[]
        for res in reslist:                        
            # Test for failure and break
            test=True
            for k,v in content.items():                
                # if this resource does not contain this attribute move on
                if not k in res.attributes:
                    test = False
                    break
                
                att = getattr(res, k, None)
                
                # Bogus - can't send lcstate objects in a dict must convert to sting to test
                if isinstance(att, registry.LCState):
                    att = str(att)
                
                print k, v
                if isinstance(v, (str, unicode) ):
                    # Use regex
                    if not re.search(v, att):
                        test=False
                        break
                else:
                    # test equality
                    #@TODO add tests for range and in list...
                    
                    
                    if att != v and v != None:
                       test=False
                       break                    
            if test:
                find_list.append(res)

        yield self.reply_ok(msg, {'res_enc':list([res.encode() for res in find_list])} )

class ResourceRegistryService(BaseResourceRegistryService):
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
        logging.info('Got Resource:\n'+str(resource))
        if resource:
            yield self.reply_ok(msg, {'res_enc':resource.encode()})
        else:
            yield self.reply_err(msg, {'res_enc':None})
    

class BaseRegistryClient(BaseServiceClient):
    """
    Do not instantiate this class!
    """

    @defer.inlineCallbacks
    def set_lcstate(self, res_id, lcstate):
        """
        @brief Retrieve a resource from the registry by its ID
        @param res_id is a resource identifier unique to this resource
        @param lcstate is a resource life cycle stae
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('set_resource_lcstate',
                                                      {'res_id':res_id,'lifecycle':str(lcstate)})
        logging.info('Service reply: '+str(content))
        
        if content['status'] == 'OK':
            defer.returnValue(True)
        else:
            defer.returnValue(False)

    def set_lcstate_new(self, res_id):
        return self.set_lcstate(res_id, registry.LCStates.new)

    def set_lcstate_active(self, res_id):
        return self.set_lcstate(res_id, registry.LCStates.active)
        
    def set_lcstate_inactive(self, res_id):
        return self.set_lcstate(res_id, registry.LCStates.inactive)

    def set_lcstate_decomm(self, res_id):
        return self.set_lcstate(res_id, registry.LCStates.decomm)

    def set_lcstate_retired(self, res_id):
        return self.set_lcstate(res_id, registry.LCStates.retired)

    def set_lcstate_developed(self, res_id):
        return self.set_lcstate(res_id, registry.LCStates.developed)

    def set_lcstate_commissioned(self, res_id):
        return self.set_lcstate(res_id, registry.LCStates.commissioned)
    


    @defer.inlineCallbacks
    def find_resources(self,attributes):
        """
        @brief Retrieve all the resources in the registry
        @param attributes is a dictionary of attributes which will be used to select a resource
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('find_resources',attributes)
        logging.info('Service reply: '+str(content))
        
        res_enc = content['res_enc']
        resources=[]
        if res_enc != None:
            for res in res_enc:
                resources.append(registry.ResourceDescription.decode(res)())
        defer.returnValue(resources)

        

class ResourceRegistryClient(BaseRegistryClient):
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
        """
        @brief Store a resource in the registry by its ID. It can be new or
        modified.
        @param res_id is a resource identifier unique to this resource.
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('register_resource',
                                            {'res_id':res_id,'res_enc':resource.encode()})
        logging.info('Service reply: '+str(headers))
        defer.returnValue(str(content['res_id']))

    @defer.inlineCallbacks
    def get_resource(self, res_id):
        """
        @brief Retrieve a resource from the registry by its ID
        @param res_id is a resource identifier unique to this resource
        """
        
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
